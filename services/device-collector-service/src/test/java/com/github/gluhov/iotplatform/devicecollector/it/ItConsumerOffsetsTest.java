package com.github.gluhov.iotplatform.devicecollector.it;

import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import com.github.gluhov.iotplatform.events.avro.OperationType;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Set;

import static com.github.gluhov.iotplatform.devicecollector.data.TestData.command;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.info;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Смещения группы в теме devices (ADR-0011, FR-022): тема хранит сообщения по сроку, а не сжимает их.
 * <ul>
 *   <li>новая группа (или раздел без смещения) читает тему с начала;</li>
 *   <li>повторное чтение темы с начала после восстановления шарда из копии даёт то же состояние, что первая обработка;</li>
 *   <li>смещение группы вне диапазона темы (часть сообщений удалена по сроку хранения до обработки) - не тихий переход
 *       на начало темы, а остановка обработчика и health DOWN: применять более поздние сообщения нельзя.</li>
 * </ul>
 * Обработчик останавливается и запускается через реестр listener-ов; после каждого теста он снова работает.
 */
class ItConsumerOffsetsTest extends AbstractItTest {
    private static final String LISTENER = "devices-listener";
    private static final Instant T = Instant.now().minus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MILLIS);

    @Autowired
    private KafkaListenerEndpointRegistry listeners;

    // ItDeviceCollectorTest останавливает Proxy последним тестом: контейнеры общие на JVM, поднимаем обратно
    @BeforeAll
    static void proxyReady() {
        ensureProxyReady();
    }

    // Обработчик работает и после упавшего теста: остальным классам IT он нужен
    @AfterEach
    void listenerRunning() {
        if (!listener().isRunning()) {
            listener().start();
        }
        await().atMost(WAIT).untilAsserted(() -> assertThat(get("/actuator/health").body()).contains("\"status\":\"UP\""));
    }

    @Test
    @DisplayName("FR-022: a partition without a committed offset is read from the beginning of the topic")
    void givenPartitionWithoutOffset_whenListenerStarts_thenReadFromBeginning() throws Exception {
        String id = deviceOnShard(0);
        stopListener();
        RecordMetadata created = send(id, command(id, "cmd-1", OperationType.CREATE, T).build(), null);
        try (Admin admin = admin()) {
            await().atMost(WAIT).ignoreExceptions().untilAsserted(() -> admin.deleteConsumerGroupOffsets(GROUP,
                    Set.of(partitionOf(created))).all().get());
        }

        listener().start();

        awaitCommitted(created);
        assertOnlyOnExpectedShard(id);
    }

    @Test
    @DisplayName("FR-022: replaying the topic from the beginning after a shard restore keeps the admin delete")
    void givenAdminDeleteThenRouterEvent_whenTopicReplayedFromBeginning_thenStillDeleted() throws Exception {
        assertThat(topicConfig(DEVICES, "cleanup.policy")).isEqualTo("delete");
        assertThat(topicConfig(DEVICES, "retention.ms")).isEqualTo("2592000000");
        String id = deviceOnShard(1);
        send(id, command(id, "cmd-1", OperationType.DELETE, T).build(), null);
        RecordMetadata router = send(id, info(id, "evt-1", DeviceUpsertEventType.UPDATED, T.plusSeconds(10)).build(), null);
        awaitCommitted(router);
        int shard = assertOnlyOnExpectedShard(id);

        // Восстановление шарда из копии, снятой до обеих записей: строк устройства нет, группа читает раздел с начала
        stopListener();
        onShard(shard, "DELETE FROM processed_device_events WHERE device_id = '" + id + "'");
        onShard(shard, "DELETE FROM devices WHERE device_id = '" + id + "'");
        TopicPartition partition = partitionOf(router);
        try (Admin admin = admin()) {
            long beginning = admin.listOffsets(Map.of(partition, OffsetSpec.earliest())).all().get()
                    .get(partition).offset();
            await().atMost(WAIT).ignoreExceptions().untilAsserted(() -> admin.alterConsumerGroupOffsets(GROUP,
                    Map.of(partition, new OffsetAndMetadata(beginning))).all().get());
        }

        listener().start();

        awaitCommitted(router);
        assertThat(onShard(shard, "SELECT status, deleted_by FROM devices WHERE device_id = '" + id + "'"))
                .isEqualTo("DELETED|ADMIN");
        assertThat(onShard(shard, "SELECT string_agg(event_key || ':' || outcome, ',' ORDER BY event_key) "
                + "FROM processed_device_events WHERE device_id = '" + id + "'"))
                .isEqualTo("cmd-1:APPLIED,evt-1:IGNORED");
    }

    @Test
    @DisplayName("FR-022: committed offset out of range (messages removed by retention) stops the listener, health DOWN")
    void givenCommittedOffsetOutOfRange_whenListenerStarts_thenStoppedAndNothingApplied() throws Exception {
        String id = deviceOnShard(0);
        awaitCommitted(send(id, command(id, "cmd-1", OperationType.CREATE, T).build(), null));
        int shard = assertOnlyOnExpectedShard(id);

        // Обработчик отстал: DELETE и более позднее событие роутера пришли, пока он стоял, а DELETE вышел за срок хранения
        stopListener();
        send(id, command(id, "cmd-2", OperationType.DELETE, T.plusSeconds(1)).build(), null);
        RecordMetadata router = send(id, info(id, "evt-1", DeviceUpsertEventType.UPDATED, T.plusSeconds(10)).build(), null);
        TopicPartition partition = partitionOf(router);
        try (Admin admin = admin()) {
            admin.deleteRecords(Map.of(partition, RecordsToDelete.beforeOffset(router.offset()))).all().get();
        }

        listener().start();

        await().atMost(WAIT).untilAsserted(() -> assertThat(listener().isRunning()).isFalse());
        HttpResponse<String> health = get("/actuator/health");
        assertThat(health.statusCode()).isEqualTo(503);
        assertThat(health.body()).contains("\"status\":\"DOWN\"");
        // Событие роутера не применено: устройство не «пережило» потерянный DELETE молча
        assertThat(onShard(shard, "SELECT count(*) FROM processed_device_events WHERE device_id = '" + id
                + "' AND event_key = 'evt-1'")).isEqualTo("0");

        // Решение оператора (например, после восстановления шарда): смещение группы - на конец раздела
        try (Admin admin = admin()) {
            long end = admin.listOffsets(Map.of(partition, OffsetSpec.latest())).all().get().get(partition).offset();
            await().atMost(WAIT).ignoreExceptions().untilAsserted(() -> admin.alterConsumerGroupOffsets(GROUP,
                    Map.of(partition, new OffsetAndMetadata(end))).all().get());
        }
    }

    private MessageListenerContainer listener() {
        MessageListenerContainer container = listeners.getListenerContainer(LISTENER);
        assertThat(container).as(LISTENER).isNotNull();
        return container;
    }

    private void stopListener() {
        listener().stop();
        await().atMost(WAIT).untilAsserted(() -> assertThat(listener().isRunning()).isFalse());
    }

    private static TopicPartition partitionOf(RecordMetadata message) {
        return new TopicPartition(message.topic(), message.partition());
    }

    private static Admin admin() {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()));
    }
}
