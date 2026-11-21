package com.github.gluhov.iotplatform.devicecollector.it;

import com.github.gluhov.iotplatform.devicecollector.DeviceCollectorApplication;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.health.actuate.endpoint.HealthEndpoint;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.test.context.event.ApplicationEvents;
import org.springframework.test.context.event.RecordApplicationEvents;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import com.github.gluhov.iotplatform.events.avro.OperationType;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
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
import java.nio.charset.StandardCharsets;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.github.gluhov.iotplatform.devicecollector.data.TestData.command;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.info;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Смещения группы в теме devices (ADR-0011, FR-022): тема хранит сообщения по сроку, а не сжимает их.
 * <ul>
 *   <li>новая группа (или раздел без смещения) читает тему с начала, в том числе раздел, добавленный к теме у\n *       работающей группы (R16): без перезапуска обработчика;</li>
 *   <li>повторное чтение темы с начала после восстановления шарда из копии даёт то же состояние, что первая обработка;</li>
 *   <li>смещение группы вне диапазона темы (часть сообщений удалена по сроку хранения до обработки) - не тихий переход
 *       на начало темы, а остановка обработчика и health DOWN: применять более поздние сообщения нельзя.</li>
 * </ul>
 * Обработчик останавливается и запускается через реестр listener-ов; после каждого теста он снова работает.
 */
@RecordApplicationEvents
class ItConsumerOffsetsTest extends AbstractItTest {
    private static final String LISTENER = "devices-listener";
    private static final Instant T = Instant.now().minus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MILLIS);

    @Autowired
    private KafkaListenerEndpointRegistry listeners;

    @Autowired
    private ApplicationEvents events;

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

    // R18: адаптер @KafkaListener - ConsumerSeekAware, Spring вызывает position() при назначении раздела, и
    // NoOffsetForPartitionException останавливает контейнер до обработчика ошибок (R16 не подтвердилось). Обёртка
    // Consumer (NoOffsetRecoveringConsumer) переводит такие разделы в начало, не останавливая обработчик
    @Test
    @DisplayName("FR-022, R18 (a): one instance - a partition added while the listener runs is read from the beginning, the listener is not stopped")
    void givenPartitionAddedWhileRunning_whenMessageSent_thenAppliedWithoutStop() throws Exception {
        int partitions = addPartition();
        // Ключ, который хэш-разбиение кладёт именно в новый раздел
        String id = deviceInNewPartition(partitions + 1);

        RecordMetadata created = send(id, command(id, "cmd-1", OperationType.CREATE, T).build(), null);

        assertThat(created.partition()).isEqualTo(partitions);
        awaitCommitted(created);
        assertOnlyOnExpectedShard(id);
        assertThat(listener().isRunning()).isTrue();
        assertThat(get("/actuator/health").body()).contains("\"status\":\"UP\"");
        assertThat(events.stream(ConsumerStoppedEvent.class).map(ConsumerStoppedEvent::getReason))
                .doesNotContain(ConsumerStoppedEvent.Reason.NO_OFFSET);
    }

    @Test
    @DisplayName("FR-022, R18 (b): two instances in one group - a partition added to the topic is applied within 60 s,"
            + " both listeners run, health UP")
    void givenTwoInstances_whenPartitionAdded_thenAppliedAndBothRunning() throws Exception {
        try (ConfigurableApplicationContext second = secondInstance()) {
            KafkaListenerEndpointRegistry secondListeners = second.getBean(KafkaListenerEndpointRegistry.class);
            await().atMost(WAIT).untilAsserted(() -> {
                assertThat(listeners.getListenerContainer(LISTENER).getAssignedPartitions()).isNotEmpty();
                assertThat(secondListeners.getListenerContainer(LISTENER).getAssignedPartitions()).isNotEmpty();
            });
            int partitions = addPartition();
            String id = deviceInNewPartition(partitions + 1);

            RecordMetadata created = send(id, command(id, "cmd-1", OperationType.CREATE, T).build(), null);

            assertThat(created.partition()).isEqualTo(partitions);
            awaitCommitted(created);
            assertOnlyOnExpectedShard(id);
            assertThat(listener().isRunning()).as("first instance").isTrue();
            assertThat(secondListeners.getListenerContainer(LISTENER).isRunning()).as("second instance").isTrue();
            assertThat(get("/actuator/health").body()).contains("\"status\":\"UP\"");
            assertThat(second.getBean(HealthEndpoint.class).health().getStatus().getCode()).isEqualTo("UP");
        }
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

    // Раздел добавляется и к devices-dlt (номер раздела исходного сообщения сохраняется); возвращает прежнее число разделов
    private static int addPartition() throws Exception {
        try (Admin admin = admin()) {
            int partitions = admin.describeTopics(List.of(DEVICES)).allTopicNames().get().get(DEVICES).partitions().size();
            admin.createPartitions(Map.of(DEVICES, NewPartitions.increaseTo(partitions + 1),
                    DLT, NewPartitions.increaseTo(partitions + 1))).all().get();
            await().atMost(WAIT).untilAsserted(() -> assertThat(admin.describeTopics(List.of(DEVICES))
                    .allTopicNames().get().get(DEVICES).partitions()).hasSize(partitions + 1));
            return partitions;
        }
    }

    // Второй экземпляр сервиса: второй контекст Spring с теми же контейнерами и группой
    private ConfigurableApplicationContext secondInstance() {
        Map<String, Object> environment = Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", KAFKA.getBootstrapServers(),
                "SCHEMA_REGISTRY_URL", schemaRegistryUrl(),
                "DEVICES_DB_URL", proxyUrl(),
                "DEVICES_DB_USERNAME", PROXY_USER,
                "DEVICES_DB_PASSWORD", PROXY_PASSWORD,
                "DEVICE_COLLECTOR_CONSUMER_GROUP", GROUP,
                "spring.data.redis.host", REDIS.getHost(),
                "spring.data.redis.port", REDIS.getMappedPort(6379));
        SpringApplication application = new SpringApplicationBuilder(DeviceCollectorApplication.class)
                .profiles("test")
                .properties("server.port=0")
                .build();
        application.setDefaultProperties(environment);
        return application.run();
    }

    // Устройство, чей ключ при partitions разделах попадает в последний раздел (хэш Kafka - как у продюсера)
    private static String deviceInNewPartition(int partitions) {
        while (true) {
            String id = deviceOnShard(0);
            int partition = Utils.toPositive(Utils.murmur2(id.getBytes(StandardCharsets.UTF_8))) % partitions;
            if (partition == partitions - 1) {
                return id;
            }
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
