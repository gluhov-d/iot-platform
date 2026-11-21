package com.github.gluhov.iotplatform.failedeventsprocessor.it;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/** Запись failed-events -> объект в MinIO (FR-007, FR-008, FR-010). */
class ItFailedEventsProcessorTest extends AbstractItTest {

    @Test
    void givenFailedEvent_whenProcessed_thenObjectWithFullJsonAndByteExactValue() throws Exception {
        long offset = nextSourceOffset();
        RecordMetadata sent = send(FAILED + "-" + offset, event(offset, null, Instant.parse("2026-11-21T10:15:31.480Z")));

        awaitCommitted(sent);

        String key = sourceKey(offset);
        assertThat(exists(key)).isTrue();
        JsonNode doc = readJson(key);
        assertThat(doc.get("schemaVersion").asInt()).isEqualTo(1);
        assertThat(doc.get("sourceService").asString()).isEqualTo("events-collector-service");
        assertThat(doc.get("sourceTopic").asString()).isEqualTo("events");
        assertThat(doc.get("sourceOffset").asLong()).isEqualTo(offset);
        assertThat(doc.get("sourceTimestamp").asString()).isEqualTo("2026-11-21T10:15:30.123Z");
        assertThat(doc.get("failedAt").asString()).isEqualTo("2026-11-21T10:15:31.480Z");
        assertThat(doc.get("errorCode").asString()).isEqualTo("VALIDATION_FAILED");
        assertThat(doc.get("errorType").asString()).isEqualTo("PERMANENT");
        assertThat(doc.get("description").asString()).isEqualTo("InvalidEventException: value must be finite");
        assertThat(doc.get("traceId").isNull()).isTrue();
        assertThat(doc.get("deviceId").asString()).isEqualTo("dev-42");
        assertThat(new String(Base64.getDecoder().decode(doc.get("key").asString()), StandardCharsets.UTF_8))
                .isEqualTo("dev-42");
        assertThat(Base64.getDecoder().decode(doc.get("value").asString()))
                .containsExactly(0, 0, 0, 0, 1, 12, 'd', 'e', 'v', (byte) 0xFF);
        assertThat(doc.get("archive").get("failedEventsTopic").asString()).isEqualTo(FAILED);
        assertThat(head(key).metadata()).containsEntry("error-code", "VALIDATION_FAILED")
                .containsEntry("source-service", "events-collector-service");
    }

    @Test
    void givenSameSourceMessageTwice_whenProcessed_thenOneObjectOneVersion() throws Exception {
        long offset = nextSourceOffset();
        send(FAILED + "-" + offset, event(offset, null, Instant.parse("2026-11-21T10:15:31.480Z")));
        // Повторная публикация того же сообщения: другое время ошибки и трасса, те же координаты источника
        RecordMetadata repeated = send(FAILED + "-" + offset,
                event(offset, "0af7651916cd43dd8448eb211c80319c", Instant.parse("2026-11-21T12:00:00Z")));

        awaitCommitted(repeated);

        String key = sourceKey(offset);
        assertThat(exists(key)).isTrue();
        assertThat(versionCount(key)).as("объект с одним ключом - одна версия (FR-008)").isEqualTo(1);
    }

    /** Ключ записи - строка: невалидный UTF-8 в ключе не должен ронять обработку (ограничение - в документации). */
    @Test
    void givenInvalidUtf8Key_whenProcessed_thenUnreadableObjectCreatedAndOffsetCommitted() throws Exception {
        RecordMetadata sent = sendRawWithKey(new byte[]{(byte) 0xC3, 0x28, (byte) 0xFF}, new byte[]{0x01, 0x02, 0x03});

        awaitCommitted(sent);

        String key = unreadableKey(sent);
        awaitObject(key);
        JsonNode doc = readJson(key);
        assertThat(doc.get("kind").asString()).isEqualTo("unreadable");
        assertThat(Base64.getDecoder().decode(doc.get("value").asString())).containsExactly(0x01, 0x02, 0x03);
        assertThat(doc.get("key").isNull()).isFalse();
    }

    @Test
    void givenUnparseableBytes_whenProcessed_thenUnreadableObjectWithRawBytesAndOffsetCommitted() throws Exception {
        RecordMetadata sent = sendRaw("raw-" + nextSourceOffset(), new byte[]{0x01, 0x02, 0x03});

        awaitCommitted(sent);

        String key = unreadableKey(sent);
        awaitObject(key);
        JsonNode doc = readJson(key);
        assertThat(doc.get("kind").asString()).isEqualTo("unreadable");
        assertThat(doc.get("reason").asString()).isNotBlank();
        assertThat(doc.get("failedEventsOffset").asLong()).isEqualTo(sent.offset());
        assertThat(Base64.getDecoder().decode(doc.get("value").asString())).containsExactly(0x01, 0x02, 0x03);
        assertThat(doc.get("headers").isObject()).isTrue();
    }
}
