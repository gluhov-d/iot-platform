package com.github.gluhov.iotplatform.failedeventsprocessor.adapter.in.kafka;

import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveUnavailableException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.PutOutcome;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.port.out.FailedEventArchive;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.service.ArchiveService;
import com.github.gluhov.iotplatform.failedeventsprocessor.support.MetricsFacade;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.DeserializationException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UnreadableRecorderTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final byte[] RAW_VALUE = {0x01, 0x02, 0x03};
    private static final byte[] RAW_KEY = {(byte) 0xFF, 0x00};
    private static final Instant RECORD_TIME = Instant.parse("2026-11-21T10:20:00Z");

    @Test
    void givenUnparseableValue_whenRecovered_thenUnreadableObjectWithRawBytesAndReason() throws Exception {
        List<String> keys = new ArrayList<>();
        List<byte[]> bodies = new ArrayList<>();
        FailedEventArchive archive = (key, body, metadata) -> {
            keys.add(key);
            bodies.add(body);
            return PutOutcome.CREATED;
        };
        UnreadableRecorder recorder = new UnreadableRecorder(new ArchiveService(archive),
                new MetricsFacade(new SimpleMeterRegistry()));

        recorder.accept(record("failed-events-key", null), deserializationFailure(RAW_VALUE, false));

        assertThat(keys).containsExactly("unreadable/2026/11/21/failed-events-1-8.json");
        JsonNode doc = JSON.readTree(bodies.getFirst());
        assertThat(doc.get("kind").asString()).isEqualTo("unreadable");
        assertThat(doc.get("reason").asString()).isEqualTo("SerializationException: Unknown magic byte!");
        assertThat(doc.get("failedEventsTopic").asString()).isEqualTo("failed-events");
        assertThat(doc.get("failedEventsPartition").asInt()).isEqualTo(1);
        assertThat(doc.get("failedEventsOffset").asLong()).isEqualTo(8L);
        assertThat(doc.get("recordTimestamp").asString()).isEqualTo("2026-11-21T10:20:00Z");
        assertThat(Base64.getDecoder().decode(doc.get("value").asString())).isEqualTo(RAW_VALUE);
        assertThat(new String(Base64.getDecoder().decode(doc.get("key").asString()), StandardCharsets.UTF_8))
                .isEqualTo("failed-events-key");
    }

    @Test
    void givenArchiveUnavailable_whenRecovered_thenRethrownAndCountedAsArchiveError() {
        FailedEventArchive archive = (key, body, metadata) -> {
            throw new ArchiveUnavailableException("timeout", null);
        };
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        UnreadableRecorder recorder = new UnreadableRecorder(new ArchiveService(archive), new MetricsFacade(registry));

        assertThatThrownBy(() -> recorder.accept(record(null, RAW_VALUE), deserializationFailure(RAW_VALUE, false)))
                .isInstanceOf(ArchiveUnavailableException.class);

        assertThat(registry.get("failed.events.archive.errors").tag("reason", "s3_unavailable")
                .counter().count()).isEqualTo(1.0);
    }

    @Test
    void givenUnreadableRecordCreated_whenRecovered_thenCountedAsUnreadable() {
        FailedEventArchive archive = (key, body, metadata) -> PutOutcome.CREATED;
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        UnreadableRecorder recorder = new UnreadableRecorder(new ArchiveService(archive), new MetricsFacade(registry));

        recorder.accept(record(null, RAW_VALUE), deserializationFailure(RAW_VALUE, false));

        assertThat(registry.get("failed.events.archived").tag("outcome", "unreadable").counter().count())
                .isEqualTo(1.0);
        // Метки фиксированы решением владельца: источник и код неизвестны
        assertThat(registry.get("failed.events.archived").tag("source_service", "unknown")
                .tag("error_code", "UNREADABLE").tag("outcome", "unreadable").counter().count())
                .isEqualTo(1.0);
    }

    @Test
    void givenUnreadableRecordRepeated_whenArchiveReturnsDuplicate_thenCountedAsDuplicateUnreadable() {
        FailedEventArchive archive = (key, body, metadata) -> PutOutcome.DUPLICATE;
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        UnreadableRecorder recorder = new UnreadableRecorder(new ArchiveService(archive), new MetricsFacade(registry));

        recorder.accept(record(null, RAW_VALUE), deserializationFailure(RAW_VALUE, false));

        assertThat(registry.get("failed.events.archived").tag("source_service", "unknown")
                .tag("error_code", "UNREADABLE").tag("outcome", "duplicate").counter().count())
                .isEqualTo(1.0);
    }

    private static DeserializationException deserializationFailure(byte[] data, boolean isKey) {
        return new DeserializationException("не разобрано", data, isKey,
                new SerializationException("Unknown magic byte!"));
    }

    /** Запись failed-events: ключ - строка, значение не разобрано (null), как после ErrorHandlingDeserializer. */
    private static ConsumerRecord<String, Object> record(String key, byte[] rawValue) {
        return new ConsumerRecord<>("failed-events", 1, 8L, RECORD_TIME.toEpochMilli(), TimestampType.CREATE_TIME,
                key == null ? 0 : key.length(), rawValue == null ? 0 : rawValue.length,
                key, null, new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());
    }
}
