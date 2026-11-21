package com.github.gluhov.iotplatform.eventscollector.adapter.out.kafka;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.events.avro.ErrorCode;
import com.github.gluhov.iotplatform.events.avro.ErrorType;
import com.github.gluhov.iotplatform.events.avro.FailedEvent;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.EventPersistenceException;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.InvalidEventException;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.CollectorMetrics;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.github.gluhov.iotplatform.eventscollector.config.RawKeepingDeserializer.RAW_KEY_HEADER;
import static com.github.gluhov.iotplatform.eventscollector.config.RawKeepingDeserializer.RAW_VALUE_HEADER;
import static com.github.gluhov.iotplatform.eventscollector.data.TestData.event;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FailedEventRecovererTest {
    private static final String FAILED_TOPIC = "failed-events";
    private static final String SOURCE_SERVICE = "events-collector-service";
    private static final Duration PUBLISH_TIMEOUT = Duration.ofSeconds(15);
    private static final long SOURCE_TIMESTAMP = 1_763_720_130_000L;
    private static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    @SuppressWarnings("unchecked")
    private final KafkaOperations<String, Object> template = mock(KafkaOperations.class);
    private final Tracer tracer = mock(Tracer.class);
    private final CollectorMetrics metrics = mock(CollectorMetrics.class);
    private final FailedEventRecoverer recoverer = new FailedEventRecoverer(
            template, tracer, metrics, FAILED_TOPIC, SOURCE_SERVICE, PUBLISH_TIMEOUT);

    @BeforeEach
    void setUp() {
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
    }

    @Test
    @DisplayName("Not deserializable message: DESERIALIZATION_FAILED, permanent, one attempt, key and value bytes as received")
    void givenDeserializationFailure_whenRecover_thenFailedEventHoldsOriginalBytes() {
        byte[] key = {(byte) 0xFF, (byte) 0xFE, 0x41};
        byte[] value = {1, 2, 3, 0x7F};
        ConsumerRecord<Object, Object> record = record(null, rawHeaders(key, value));
        DeserializationException poison = new DeserializationException("not avro", value, false,
                new SerializationException("Unknown magic byte!"));

        recoverer.accept(record, new ListenerExecutionFailedException("listener failed", poison));

        FailedEvent event = published();
        assertThat(event.getErrorCode()).isEqualTo(ErrorCode.DESERIALIZATION_FAILED);
        assertThat(event.getErrorType()).isEqualTo(ErrorType.PERMANENT);
        assertThat(event.getAttempts()).isEqualTo(1);
        assertThat(event.getDeviceId()).isNull();
        assertThat(bytes(event.getKey())).isEqualTo(key);
        assertThat(bytes(event.getValue())).isEqualTo(value);
        verify(metrics).failedEventPublished("DESERIALIZATION_FAILED", "PERMANENT");
    }

    @Test
    @DisplayName("Invalid event: VALIDATION_FAILED, permanent, one attempt, device id taken from the parsed message")
    void givenInvalidEvent_whenRecover_thenValidationFailedWithDeviceId() {
        ConsumerRecord<Object, Object> record = record(event("evt-1", "dev-1", 1_000L), rawHeaders(null, new byte[]{9, 9}));

        recoverer.accept(record, new ListenerExecutionFailedException("listener failed",
                new InvalidEventException("eventId is blank")));

        FailedEvent event = published();
        assertThat(event.getErrorCode()).isEqualTo(ErrorCode.VALIDATION_FAILED);
        assertThat(event.getErrorType()).isEqualTo(ErrorType.PERMANENT);
        assertThat(event.getAttempts()).isEqualTo(1);
        assertThat(event.getDeviceId()).isEqualTo("dev-1");
        assertThat(event.getDescription()).contains("InvalidEventException").contains("eventId is blank");
    }

    @Test
    @DisplayName("Storage failed after all attempts: STORAGE_UNAVAILABLE, transient, attempts from the delivery header")
    void givenStorageFailureAfterAttempts_whenRecover_thenStorageUnavailableWithDeliveryAttempts() {
        RecordHeaders headers = rawHeaders(null, new byte[]{1});
        headers.add(KafkaHeaders.DELIVERY_ATTEMPT, intBytes(3));
        ConsumerRecord<Object, Object> record = record(event("evt-1", "dev-1", 1_000L), headers);

        recoverer.accept(record, new ListenerExecutionFailedException("listener failed",
                new EventPersistenceException("insert device_events failed", new IllegalStateException("clickhouse is down"))));

        FailedEvent event = published();
        assertThat(event.getErrorCode()).isEqualTo(ErrorCode.STORAGE_UNAVAILABLE);
        assertThat(event.getErrorType()).isEqualTo(ErrorType.TRANSIENT);
        assertThat(event.getAttempts()).isEqualTo(3);
        assertThat(event.getDescription()).isEqualTo("java.lang.IllegalStateException: clickhouse is down");
    }

    @Test
    @DisplayName("Other error after all attempts: PROCESSING_FAILED, transient, attempts from the delivery header")
    void givenUnexpectedError_whenRecover_thenProcessingFailed() {
        RecordHeaders headers = rawHeaders(null, new byte[]{1});
        headers.add(KafkaHeaders.DELIVERY_ATTEMPT, intBytes(2));
        ConsumerRecord<Object, Object> record = record(event("evt-1", "dev-1", 1_000L), headers);

        recoverer.accept(record, new ListenerExecutionFailedException("listener failed", new IllegalStateException("bug")));

        FailedEvent event = published();
        assertThat(event.getErrorCode()).isEqualTo(ErrorCode.PROCESSING_FAILED);
        assertThat(event.getErrorType()).isEqualTo(ErrorType.TRANSIENT);
        assertThat(event.getAttempts()).isEqualTo(2);
    }

    @Test
    @DisplayName("Coordinates, source timestamp and service are copied; key of the message is topic-partition-offset")
    void givenRecord_whenRecover_thenCoordinatesAndTimestampsFilled() {
        ConsumerRecord<Object, Object> record = record(event("evt-1", "dev-1", 1_000L), rawHeaders(null, new byte[]{1}));

        // failedAt хранится в миллисекундах, как timestamp-millis в схеме
        Instant before = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        recoverer.accept(record, new ListenerExecutionFailedException("listener failed",
                new InvalidEventException("bad")));

        ProducerRecord<String, Object> sent = sent();
        assertThat(sent.key()).isEqualTo("events-2-42");
        FailedEvent event = (FailedEvent) sent.value();
        assertThat(event.getSourceService()).isEqualTo(SOURCE_SERVICE);
        assertThat(event.getSourceTopic()).isEqualTo("events");
        assertThat(event.getSourcePartition()).isEqualTo(2);
        assertThat(event.getSourceOffset()).isEqualTo(42L);
        assertThat(event.getSourceTimestamp()).isEqualTo(Instant.ofEpochMilli(SOURCE_TIMESTAMP));
        assertThat(event.getFailedAt()).isBetween(before, Instant.now());
    }

    @Test
    @DisplayName("Trace from the current span is used when the span is present")
    void givenCurrentSpan_whenRecover_thenTraceIdFromSpan() {
        Span span = mock(Span.class);
        TraceContext context = mock(TraceContext.class);
        when(tracer.currentSpan()).thenReturn(span);
        when(span.context()).thenReturn(context);
        when(context.traceId()).thenReturn(TRACE_ID);
        RecordHeaders headers = rawHeaders(null, new byte[]{1});
        headers.add("traceparent", TRACEPARENT.getBytes());

        recoverer.accept(record(event("evt-1", "dev-1", 1_000L), headers), new ListenerExecutionFailedException("x",
                new InvalidEventException("bad")));

        assertThat(published().getTraceId()).isEqualTo(TRACE_ID);
    }

    @Test
    @DisplayName("Without a current span the trace is taken from the traceparent header")
    void givenOnlyTraceparentHeader_whenRecover_thenTraceIdFromHeader() {
        RecordHeaders headers = rawHeaders(null, new byte[]{1});
        headers.add("traceparent", TRACEPARENT.getBytes());

        recoverer.accept(record(event("evt-1", "dev-1", 1_000L), headers), new ListenerExecutionFailedException("x",
                new InvalidEventException("bad")));

        assertThat(published().getTraceId()).isEqualTo("0af7651916cd43dd8448eb211c80319c");
    }

    @Test
    @DisplayName("Without a span and without traceparent the trace id is null")
    void givenNoTrace_whenRecover_thenTraceIdNull() {
        recoverer.accept(record(event("evt-1", "dev-1", 1_000L), rawHeaders(null, new byte[]{1})),
                new ListenerExecutionFailedException("x", new InvalidEventException("bad")));

        assertThat(published().getTraceId()).isNull();
    }

    @Test
    @DisplayName("Only the source headers are kept: no Spring or retry headers, no raw-byte helper headers")
    void givenServiceHeaders_whenRecover_thenOnlySourceHeadersKept() {
        RecordHeaders headers = rawHeaders(null, new byte[]{1});
        headers.add("traceparent", TRACEPARENT.getBytes());
        headers.add("springDeserializerExceptionValue", new byte[]{5});
        headers.add(KafkaHeaders.DELIVERY_ATTEMPT, intBytes(1));
        headers.add("kafka_custom", new byte[]{6});

        recoverer.accept(record(event("evt-1", "dev-1", 1_000L), headers), new ListenerExecutionFailedException("x",
                new InvalidEventException("bad")));

        assertThat(published().getHeaders()).containsOnlyKeys("traceparent");
    }

    @Test
    @DisplayName("Description is the root cause class and message, cut to 2000 characters")
    void givenLongMessage_whenRecover_thenDescriptionCut() {
        recoverer.accept(record(event("evt-1", "dev-1", 1_000L), rawHeaders(null, new byte[]{1})),
                new ListenerExecutionFailedException("x", new IllegalStateException("x".repeat(5_000))));

        assertThat(published().getDescription()).hasSize(2_000).startsWith("java.lang.IllegalStateException: ");
    }

    @Test
    @DisplayName("Publish fails: exception and no metric, so the offset is not committed")
    void givenPublishFails_whenRecover_thenExceptionAndNoMetric() {
        when(template.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("failed-events is down")));

        assertThatThrownBy(() -> recoverer.accept(record(event("evt-1", "dev-1", 1_000L), rawHeaders(null, new byte[]{1})),
                new ListenerExecutionFailedException("x", new InvalidEventException("bad"))))
                .hasMessageContaining(FAILED_TOPIC);
        verify(metrics, never()).failedEventPublished(anyString(), anyString());
    }

    private static ConsumerRecord<Object, Object> record(Object value, RecordHeaders headers) {
        return new ConsumerRecord<>("events", 2, 42L, SOURCE_TIMESTAMP, TimestampType.CREATE_TIME,
                -1, -1, "dev-1", value, headers, Optional.empty());
    }

    private static RecordHeaders rawHeaders(byte[] rawKey, byte[] rawValue) {
        RecordHeaders headers = new RecordHeaders();
        if (rawKey != null) {
            headers.add(RAW_KEY_HEADER, rawKey);
        }
        headers.add(RAW_VALUE_HEADER, rawValue);
        return headers;
    }

    private static byte[] intBytes(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] result = new byte[copy.remaining()];
        copy.get(result);
        return result;
    }

    private ProducerRecord<String, Object> sent() {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, Object>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(sent.capture());
        assertThat(sent.getValue().topic()).isEqualTo(FAILED_TOPIC);
        return sent.getValue();
    }

    private FailedEvent published() {
        return (FailedEvent) sent().value();
    }
}
