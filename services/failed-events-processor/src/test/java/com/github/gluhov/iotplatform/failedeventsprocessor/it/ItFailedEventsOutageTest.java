package com.github.gluhov.iotplatform.failedeventsprocessor.it;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import tools.jackson.databind.JsonNode;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Недоступность MinIO и Schema Registry (FR-009, FR-010; R6, R7). Паузы - docker pause: адрес и порт не меняются.
 * Пауза MinIO дольше max.poll.interval.ms (30 с): группа должна остаться Stable за счёт тайм-аутов клиента и паузы контейнера.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ItFailedEventsOutageTest extends AbstractItTest {

    private static final Duration MINIO_PAUSE = Duration.ofSeconds(45);
    private static final Duration REGISTRY_PAUSE = Duration.ofSeconds(20);

    @Test
    @Order(1)
    void givenMinioPausedLongerThanMaxPollInterval_whenProcessed_thenGroupStableAndObjectAfterUnpause() throws Exception {
        long offset = nextSourceOffset();
        long committedBefore = committedOffset();
        pause(MINIO);
        RecordMetadata sent;
        try {
            sent = send(FAILED + "-" + offset, event(offset, null, Instant.parse("2026-11-21T10:15:31.480Z")));
            Instant until = Instant.now().plus(MINIO_PAUSE);
            while (Instant.now().isBefore(until)) {
                Thread.sleep(5_000);
                assertThat(groupState()).as("группа Stable во время паузы MinIO").isEqualToIgnoringCase("Stable");
                assertThat(committedOffset()).as("смещение не движется во время паузы").isEqualTo(committedBefore);
            }
        } finally {
            unpause(MINIO);
        }

        String key = objectKey(sent);
        awaitObject(key);
        awaitCommitted(sent);
        assertThat(readJson(key).get("sourceOffset").asLong()).isEqualTo(offset);
    }

    @Test
    @Order(2)
    void givenRegistryPaused_whenProcessed_thenNoUnreadableWhileDownAndUnreadableAfterUnpause() throws Exception {
        // Схема 99999 не зарегистрирована: при работающем Schema Registry - 404, то есть нечитаемая запись.
        // Пауза ставится ДО отправки: иначе запись могла бы разобраться раньше паузы.
        RecordMetadata sent;
        long committedBefore = committedOffset();
        pause(SCHEMA_REGISTRY);
        try {
            sent = sendRaw("sr-down-" + nextSourceOffset(), new byte[]{0, 0, 0, 0x27, 0x0F, 1, 2, 3});
            String key = objectKey(sent);
            Instant until = Instant.now().plus(REGISTRY_PAUSE);
            while (Instant.now().isBefore(until)) {
                Thread.sleep(5_000);
                assertThat(groupState()).as("группа Stable во время паузы Schema Registry").isEqualToIgnoringCase("Stable");
                assertThat(committedOffset()).as("смещение не движется во время паузы Schema Registry")
                        .isEqualTo(committedBefore);
                assertThat(exists(key)).as("во время паузы Schema Registry unreadable не создан").isFalse();
            }
        } finally {
            unpause(SCHEMA_REGISTRY);
        }

        String key = objectKey(sent);
        awaitObject(key);
        awaitCommitted(sent);
        JsonNode doc = readJson(key);
        assertThat(doc.get("kind").asString()).isEqualTo("unreadable");
        assertThat(doc.get("reason").asString()).contains("RestClientException");
    }
}
