package com.github.gluhov.iotplatform.failedeventsprocessor.domain.service;

import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.ArchiveCoordinates;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.FailedRecord;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.HeaderEntry;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.PutOutcome;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.UnreadableRecord;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.port.out.FailedEventArchive;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ArchiveServiceTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final byte[] VALUE = {0, 0, 0, 0, 1, 12, 'd', 'e', 'v', (byte) 0xFF};
    private static final byte[] KEY = "dev-42".getBytes(StandardCharsets.UTF_8);

    private final List<Stored> stored = new ArrayList<>();
    private final FailedEventArchive archive = (key, body, metadata) -> {
        stored.add(new Stored(key, body, metadata));
        return PutOutcome.CREATED;
    };
    private final ArchiveService service = new ArchiveService(archive);

    @Test
    void givenFailedRecord_whenArchive_thenObjectKeyAndJsonContainAllFields() throws Exception {
        FailedRecord record = record();
        ArchiveCoordinates coords = coords();

        PutOutcome outcome = service.archive(record, coords);

        assertThat(outcome).isEqualTo(PutOutcome.CREATED);
        assertThat(stored).hasSize(1);
        Stored object = stored.getFirst();
        // Ключ - из координат и времени записи failed-events, а не исходного сообщения (ADR-0014)
        assertThat(object.key()).isEqualTo("2026/11/21/failed-events-1-7.json");

        JsonNode doc = JSON.readTree(object.body());
        assertThat(doc.get("schemaVersion").asInt()).isEqualTo(2);
        assertThat(doc.get("sourceService").asString()).isEqualTo("events-collector-service");
        assertThat(doc.get("sourceTopic").asString()).isEqualTo("events");
        assertThat(doc.get("sourcePartition").asInt()).isEqualTo(2);
        assertThat(doc.get("sourceOffset").asLong()).isEqualTo(1042L);
        assertThat(doc.get("sourceTimestamp").asString()).isEqualTo("2026-11-21T10:15:30.123Z");
        assertThat(doc.get("failedAt").asString()).isEqualTo("2026-11-21T10:15:31.480Z");
        assertThat(doc.get("errorCode").asString()).isEqualTo("VALIDATION_FAILED");
        assertThat(doc.get("errorType").asString()).isEqualTo("PERMANENT");
        assertThat(doc.get("description").asString()).isEqualTo("InvalidEventException: value must be finite");
        assertThat(doc.get("attempts").asInt()).isEqualTo(1);
        assertThat(doc.get("traceId").asString()).isEqualTo("0af7651916cd43dd8448eb211c80319c");
        assertThat(doc.get("deviceId").asString()).isEqualTo("dev-42");
        assertThat(doc.get("archive").get("failedEventsTopic").asString()).isEqualTo("failed-events");
        assertThat(doc.get("archive").get("failedEventsPartition").asInt()).isEqualTo(1);
        assertThat(doc.get("archive").get("failedEventsOffset").asLong()).isEqualTo(7L);
        assertThat(doc.get("archive").get("recordTimestamp").asString()).isEqualTo("2026-11-21T10:15:31.700Z");
        assertThat(doc.get("archive").get("archivedAt").asString()).isEqualTo("2026-11-21T10:15:31.902Z");
    }

    @Test
    void givenBinaryBytes_whenArchive_thenBase64EqualsOriginalBytes() throws Exception {
        service.archive(record(), coords());

        JsonNode doc = JSON.readTree(stored.getFirst().body());
        assertThat(Base64.getDecoder().decode(doc.get("value").asString())).isEqualTo(VALUE);
        assertThat(Base64.getDecoder().decode(doc.get("key").asString())).isEqualTo(KEY);
    }

    @Test
    void givenNullTraceIdKeyAndValue_whenArchive_thenJsonHasNulls() throws Exception {
        FailedRecord record = new FailedRecord("events-collector-service", "events", 0, 1L,
                Instant.parse("2026-11-21T10:15:30Z"), Instant.parse("2026-11-21T10:15:31Z"),
                "DESERIALIZATION_FAILED", "PERMANENT", "SerializationException: Unknown magic byte!",
                null, null, null, List.of(), 1, null);

        service.archive(record, coords());

        JsonNode doc = JSON.readTree(stored.getFirst().body());
        assertThat(doc.get("traceId").isNull()).isTrue();
        assertThat(doc.get("key").isNull()).isTrue();
        assertThat(doc.get("value").isNull()).isTrue();
        assertThat(doc.get("deviceId").isNull()).isTrue();
    }

    @Test
    void givenHeaders_whenArchive_thenHeadersAreBase64ArrayInSourceOrder() throws Exception {
        service.archive(record(), coords());

        JsonNode headers = JSON.readTree(stored.getFirst().body()).get("headers");
        assertThat(headers.isArray()).isTrue();
        assertThat(headers.get(0).get("name").asString()).isEqualTo("traceparent");
        assertThat(new String(Base64.getDecoder().decode(headers.get(0).get("value").asString()), StandardCharsets.UTF_8))
                .isEqualTo("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    }

    /** Повторное имя, значение null и порядок - как в записи (R26): объект хранит полный список заголовков. */
    @Test
    void givenRepeatedAndNullableHeaders_whenArchive_thenArrayKeepsRepeatsNullsAndOrder() throws Exception {
        FailedRecord record = new FailedRecord("events-collector-service", "events", 2, 1042L,
                Instant.parse("2026-11-21T10:15:30.123Z"), Instant.parse("2026-11-21T10:15:31.480Z"),
                "VALIDATION_FAILED", "PERMANENT", "InvalidEventException: value must be finite",
                null, KEY, VALUE,
                List.of(new HeaderEntry("tag", "a".getBytes(StandardCharsets.UTF_8)),
                        new HeaderEntry("tag", "b".getBytes(StandardCharsets.UTF_8)),
                        new HeaderEntry("nullable", null)),
                1, null);

        service.archive(record, coords());

        JsonNode headers = JSON.readTree(stored.getFirst().body()).get("headers");
        assertThat(headers.size()).isEqualTo(3);
        assertThat(headers.get(0).get("name").asString()).isEqualTo("tag");
        assertThat(Base64.getDecoder().decode(headers.get(0).get("value").asString())).isEqualTo("a".getBytes(StandardCharsets.UTF_8));
        assertThat(headers.get(1).get("name").asString()).isEqualTo("tag");
        assertThat(Base64.getDecoder().decode(headers.get(1).get("value").asString())).isEqualTo("b".getBytes(StandardCharsets.UTF_8));
        assertThat(headers.get(2).get("name").asString()).isEqualTo("nullable");
        assertThat(headers.get(2).get("value").isNull()).isTrue();
    }

    @Test
    void givenRecord_whenArchive_thenMetadataHasErrorCodeSourceAndTrace() {
        service.archive(record(), coords());

        assertThat(stored.getFirst().metadata())
                .containsEntry("error-code", "VALIDATION_FAILED")
                .containsEntry("source-service", "events-collector-service")
                .containsEntry("source-topic", "events")
                .containsEntry("trace-id", "0af7651916cd43dd8448eb211c80319c")
                .containsEntry("device-id", "dev-42");
    }

    @Test
    void givenNoTraceAndNoDevice_whenArchive_thenMetadataWithoutOptionalKeys() {
        FailedRecord record = new FailedRecord("events-collector-service", "events", 0, 1L,
                Instant.parse("2026-11-21T10:15:30Z"), Instant.parse("2026-11-21T10:15:31Z"),
                "UNKNOWN", "PERMANENT", "x", null, null, null, List.of(), 1, null);

        service.archive(record, coords());

        assertThat(stored.getFirst().metadata())
                .containsOnlyKeys("error-code", "source-service", "source-topic");
    }

    @Test
    void givenUnreadableRecord_whenArchive_thenKindReasonAndRawBytesInBase64() throws Exception {
        UnreadableRecord record = new UnreadableRecord("failed-events", 1, 8L,
                Instant.parse("2026-11-21T10:20:00Z"), "SerializationException: Unknown magic byte!",
                KEY, new byte[]{0x01, 0x02, 0x03},
                List.of(new HeaderEntry("traceparent", "00-x".getBytes(StandardCharsets.UTF_8))));

        PutOutcome outcome = service.archiveUnreadable(record);

        assertThat(outcome).isEqualTo(PutOutcome.CREATED);
        Stored object = stored.getFirst();
        assertThat(object.key()).isEqualTo("2026/11/21/failed-events-1-8.json");
        assertThat(object.metadata()).containsOnlyKeys("kind").containsEntry("kind", "unreadable");
        JsonNode doc = JSON.readTree(object.body());
        assertThat(doc.get("schemaVersion").asInt()).isEqualTo(2);
        assertThat(doc.get("kind").asString()).isEqualTo("unreadable");
        assertThat(doc.get("reason").asString()).isEqualTo("SerializationException: Unknown magic byte!");
        assertThat(doc.get("recordTimestamp").asString()).isEqualTo("2026-11-21T10:20:00Z");
        assertThat(doc.get("failedEventsOffset").asLong()).isEqualTo(8L);
        assertThat(Base64.getDecoder().decode(doc.get("value").asString())).containsExactly(0x01, 0x02, 0x03);
        assertThat(Base64.getDecoder().decode(doc.get("key").asString())).isEqualTo(KEY);
        assertThat(doc.get("value").asString()).isNotNull();
        assertThat(doc.get("headers").get(0).get("name").asString()).isEqualTo("traceparent");
        assertThat(new String(Base64.getDecoder().decode(doc.get("headers").get(0).get("value").asString()),
                StandardCharsets.UTF_8)).isEqualTo("00-x");
    }

    /**
     * Запись failed-events сначала нечитаемая (схема не найдена), процесс упал до подтверждения смещения, схема
     * появилась, и та же запись разобрана: ключ тот же, условная запись даёт «объект уже есть», объект один (FR-008, FR-010).
     */
    @Test
    void givenUnreadableThenReadableOfSameRecord_whenArchive_thenSameKeyAndSecondIsDuplicate() {
        Map<String, byte[]> bucket = new java.util.HashMap<>();
        FailedEventArchive conditional = (key, body, metadata) ->
                bucket.putIfAbsent(key, body) == null ? PutOutcome.CREATED : PutOutcome.DUPLICATE;
        ArchiveService conditionalService = new ArchiveService(conditional);
        UnreadableRecord unreadable = new UnreadableRecord("failed-events", 1, 7L,
                Instant.parse("2026-11-21T10:15:31.700Z"), "RestClientException: schema not found",
                KEY, VALUE, List.of());

        PutOutcome first = conditionalService.archiveUnreadable(unreadable);
        PutOutcome second = conditionalService.archive(record(), coords());

        assertThat(first).isEqualTo(PutOutcome.CREATED);
        assertThat(second).isEqualTo(PutOutcome.DUPLICATE);
        assertThat(bucket).containsOnlyKeys("2026/11/21/failed-events-1-7.json");
    }

    private static ArchiveCoordinates coords() {
        return new ArchiveCoordinates("failed-events", 1, 7L, Instant.parse("2026-11-21T10:15:31.700Z"),
                Instant.parse("2026-11-21T10:15:31.902Z"));
    }

    private static FailedRecord record() {
        return new FailedRecord("events-collector-service", "events", 2, 1042L,
                Instant.parse("2026-11-21T10:15:30.123Z"), Instant.parse("2026-11-21T10:15:31.480Z"),
                "VALIDATION_FAILED", "PERMANENT", "InvalidEventException: value must be finite",
                "0af7651916cd43dd8448eb211c80319c", KEY, VALUE,
                List.of(new HeaderEntry("traceparent",
                        "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".getBytes(StandardCharsets.UTF_8))),
                1, "dev-42");
    }

    private record Stored(String key, byte[] body, Map<String, String> metadata) {
    }
}
