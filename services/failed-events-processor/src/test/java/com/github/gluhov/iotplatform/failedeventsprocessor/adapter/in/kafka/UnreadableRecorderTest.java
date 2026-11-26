package com.github.gluhov.iotplatform.failedeventsprocessor.adapter.in.kafka;

import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveUnavailableException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.PutOutcome;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.port.out.FailedEventArchive;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.service.ArchiveService;
import com.github.gluhov.iotplatform.failedeventsprocessor.support.MetricsFacade;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
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

        recorder.accept(record("failed-events-key", RAW_VALUE), deserializationFailure(RAW_VALUE, false));

        assertThat(keys).containsExactly("2026/11/21/failed-events-1-8.json");
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

    /**
     * Ошибка без DeserializationException (ClassCastException: корректный Avro другого типа). Байты берутся только из
     * x-raw-key и x-raw-value, служебные заголовки Spring и x-raw-* в объект не попадают.
     */
    @Test
    void givenClassCastFailureWithRawHeaders_whenRecovered_thenKeyAndValueByteExactFromRawHeaders() throws Exception {
        byte[] rawKey = {(byte) 0xC3, 0x28, (byte) 0xFF};
        RecordHeaders headers = new RecordHeaders();
        headers.add("x-raw-key", rawKey);
        headers.add("x-raw-value", RAW_VALUE);
        headers.add("traceparent", new byte[]{1, 2});
        headers.add("springDeserializerExceptionValue", new byte[]{9});
        List<byte[]> bodies = new ArrayList<>();
        FailedEventArchive archive = (key, body, metadata) -> {
            bodies.add(body);
            return PutOutcome.CREATED;
        };
        UnreadableRecorder recorder = new UnreadableRecorder(new ArchiveService(archive),
                new MetricsFacade(new SimpleMeterRegistry()));

        recorder.accept(recordWith(null, headers), new ClassCastException("DeviceEvent cannot be cast to FailedEvent"));

        JsonNode doc = JSON.readTree(bodies.getFirst());
        assertThat(doc.get("kind").asString()).isEqualTo("unreadable");
        assertThat(Base64.getDecoder().decode(doc.get("key").asString())).isEqualTo(rawKey);
        assertThat(Base64.getDecoder().decode(doc.get("value").asString())).isEqualTo(RAW_VALUE);
        assertThat(doc.get("reason").asString()).startsWith("ClassCastException");
    }

    /** Заголовки объекта - только пользовательские: x-raw-* и springDeserializerException* исключены. */
    @Test
    void givenRawAndSpringHeaders_whenRecovered_thenOnlyUserHeadersInObject() throws Exception {
        RecordHeaders headers = new RecordHeaders();
        headers.add("x-raw-key", new byte[]{1});
        headers.add("x-raw-value", RAW_VALUE);
        headers.add("springDeserializerExceptionKey", new byte[]{2});
        headers.add("springDeserializerExceptionValue", new byte[]{3});
        headers.add("traceparent", new byte[]{4, 5});
        List<byte[]> bodies = new ArrayList<>();
        FailedEventArchive archive = (key, body, metadata) -> {
            bodies.add(body);
            return PutOutcome.CREATED;
        };
        UnreadableRecorder recorder = new UnreadableRecorder(new ArchiveService(archive),
                new MetricsFacade(new SimpleMeterRegistry()));

        recorder.accept(recordWith(null, headers), new ClassCastException("cast"));

        JsonNode objectHeaders = JSON.readTree(bodies.getFirst()).get("headers");
        assertThat(objectHeaders.size()).isEqualTo(1);
        assertThat(objectHeaders.has("traceparent")).isTrue();
        assertThat(objectHeaders.has("x-raw-key")).isFalse();
        assertThat(objectHeaders.has("x-raw-value")).isFalse();
    }

    private static DeserializationException deserializationFailure(byte[] data, boolean isKey) {
        return new DeserializationException("не разобрано", data, isKey,
                new SerializationException("Unknown magic byte!"));
    }

    /**
     * Запись failed-events как после обёртки RawKeepingDeserializer: сырые байты значения - в x-raw-value, ключа - в
     * x-raw-key (ключ строкой, если он валиден). Значение объекта не разобрано (null).
     */
    private static ConsumerRecord<String, Object> record(String key, byte[] rawValue) {
        RecordHeaders headers = new RecordHeaders();
        if (key != null) {
            headers.add("x-raw-key", key.getBytes(StandardCharsets.UTF_8));
        }
        if (rawValue != null) {
            headers.add("x-raw-value", rawValue);
        }
        return recordWith(key, headers);
    }

    private static ConsumerRecord<String, Object> recordWith(String key, Headers headers) {
        return new ConsumerRecord<>("failed-events", 1, 8L, RECORD_TIME.toEpochMilli(), TimestampType.CREATE_TIME,
                key == null ? 0 : key.length(), 0, key, null, headers, Optional.empty());
    }
}
