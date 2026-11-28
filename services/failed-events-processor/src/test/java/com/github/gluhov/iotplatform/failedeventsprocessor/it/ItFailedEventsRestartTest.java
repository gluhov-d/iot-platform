package com.github.gluhov.iotplatform.failedeventsprocessor.it;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Перезапуск процессора над непустым бакетом (T038): объекты остаются на месте, повторная обработка тех же записей
 * не создаёт новых версий и не перезаписывает объекты. Перезапуск - остановка контейнера слушателя, сброс смещения
 * группы в начало и повторный запуск в том же процессе (перезапуск JVM в тесте не моделируется).
 */
class ItFailedEventsRestartTest extends AbstractItTest {

    @Autowired
    private KafkaListenerEndpointRegistry listeners;

    @Test
    void givenNonEmptyBucket_whenProcessorRestartedAndReadsFromStart_thenObjectsKeptWithoutNewVersions() throws Exception {
        long offset = nextSourceOffset();
        RecordMetadata readable = send(FAILED + "-" + offset, event(offset, null, Instant.parse("2026-11-21T10:15:31.480Z")));
        RecordMetadata raw = sendRaw("restart-" + nextSourceOffset(), new byte[]{9, 9, 9});
        awaitCommitted(raw);

        String key = objectKey(readable);
        String unreadable = objectKey(raw);
        awaitObject(key);
        awaitObject(unreadable);
        String etagBefore = head(key).eTag();
        String unreadableEtagBefore = head(unreadable).eTag();
        int versionsBefore = versionCount(key);
        int unreadableVersionsBefore = versionCount(unreadable);

        MessageListenerContainer container = listeners.getListenerContainer("failed-events-processor");
        container.stop();
        await().atMost(WAIT).until(() -> !container.isRunning());
        await().atMost(WAIT).until(() -> !"Stable".equalsIgnoreCase(groupState()));
        resetGroupOffsetsToStart();
        assertThat(committedOffset()).isZero();

        container.start();
        awaitCommitted(raw);

        assertThat(exists(key)).isTrue();
        assertThat(exists(unreadable)).isTrue();
        assertThat(head(key).eTag()).as("объект не перезаписан").isEqualTo(etagBefore);
        assertThat(head(unreadable).eTag()).as("unreadable не перезаписан").isEqualTo(unreadableEtagBefore);
        assertThat(versionCount(key)).as("новых версий нет").isEqualTo(versionsBefore);
        assertThat(versionCount(unreadable)).as("новых версий нет").isEqualTo(unreadableVersionsBefore);
    }

    private static void resetGroupOffsetsToStart() throws Exception {
        try (var admin = org.apache.kafka.clients.admin.Admin.create(Map.of(
                org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.alterConsumerGroupOffsets(GROUP, Map.of(new TopicPartition(FAILED, 0), new OffsetAndMetadata(0L)))
                    .all().get();
        }
    }
}
