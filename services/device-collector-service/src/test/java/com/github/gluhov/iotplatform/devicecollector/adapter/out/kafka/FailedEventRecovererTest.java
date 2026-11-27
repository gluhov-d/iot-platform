package com.github.gluhov.iotplatform.devicecollector.adapter.out.kafka;

import com.github.gluhov.iotplatform.devicecollector.config.KafkaProperties;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.DeviceNotStoredException;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.InvalidDeviceInfoException;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.StorageAccessException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StorageFailureReason;
import com.github.gluhov.iotplatform.devicecollector.domain.port.out.DeviceMetrics;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import com.github.gluhov.iotplatform.events.avro.ErrorCode;
import com.github.gluhov.iotplatform.events.avro.ErrorType;
import com.github.gluhov.iotplatform.events.avro.FailedEvent;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.apache.kafka.common.errors.SerializationException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;

import static com.github.gluhov.iotplatform.devicecollector.data.TestData.T0;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.info;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Запись в failed-events: что публикуется для каждого типа ошибки, исходные байты, заголовки, traceId и
 * отказ публикации (смещение тогда не подтверждается). Kafka и Tracer заменены моками.
 */
class FailedEventRecovererTest {
    private static final String FAILED = "failed-events";
    private static final String TRACEPARENT_TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String SPAN_TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
    private static final String SPAN_PARENT = "00-" + TRACEPARENT_TRACE_ID + "-00f067aa0ba902b7-01";

    private final Clock clock = Clock.fixed(Instant.parse("2026-11-21T10:00:00Z"), ZoneOffset.UTC);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<Object, Object> template = mock(KafkaTemplate.class);
    private final Tracer tracer = mock(Tracer.class);
    private final DeviceMetrics metrics = mock(DeviceMetrics.class);
    private final KafkaProperties properties = new KafkaProperties("devices", FAILED, Duration.ofSeconds(15));
    private final FailedEventRecoverer recoverer = new FailedEventRecoverer(template, tracer, metrics, clock, properties);

    private ConsumerRecord<Object, Object> record;

    @BeforeEach
    void setUp() {
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        record = new ConsumerRecord<>("devices", 2, 10L, "dev-1",
                info("dev-1", "evt-1", DeviceUpsertEventType.CREATED, T0).build());
    }

    @Test
    @DisplayName("Invalid DeviceInfo: VALIDATION_FAILED, PERMANENT, one attempt, key <topic>-<partition>-<offset>")
    void givenInvalidDeviceInfo_whenRecovered_thenValidationFailedPermanentWithOneAttempt() {
        recoverer.accept(record, listenerFailure(new InvalidDeviceInfoException("deviceId is blank")));

        FailedEvent event = published();
        assertThat(sentKey()).isEqualTo("devices-2-10");
        assertThat(event.getErrorCode()).isEqualTo(ErrorCode.VALIDATION_FAILED);
        assertThat(event.getErrorType()).isEqualTo(ErrorType.PERMANENT);
        assertThat(event.getAttempts()).isEqualTo(1);
        assertThat(event.getSourceService()).isEqualTo("device-collector-service");
        assertThat(event.getSourceTopic()).isEqualTo("devices");
        assertThat(event.getSourcePartition()).isEqualTo(2);
        assertThat(event.getSourceOffset()).isEqualTo(10L);
        assertThat(event.getSourceTimestamp()).isEqualTo(Instant.ofEpochMilli(record.timestamp()));
        assertThat(event.getFailedAt()).isEqualTo(clock.instant());
        assertThat(event.getDeviceId()).isEqualTo("dev-1");
        assertThat(event.getDescription()).startsWith(InvalidDeviceInfoException.class.getName() + ": deviceId is blank");
        verify(metrics).failedEventPublished(ErrorCode.VALIDATION_FAILED, ErrorType.PERMANENT);
    }

    @Test
    @DisplayName("Operation UNKNOWN (newer than the schema): VALIDATION_FAILED, PERMANENT, one attempt")
    void givenUnknownOperation_whenRecovered_thenValidationFailed() {
        recoverer.accept(record, listenerFailure(new InvalidDeviceInfoException(
                "operation UNKNOWN: command is newer than this service's schema")));

        FailedEvent event = published();
        assertThat(event.getErrorCode()).isEqualTo(ErrorCode.VALIDATION_FAILED);
        assertThat(event.getErrorType()).isEqualTo(ErrorType.PERMANENT);
        assertThat(event.getAttempts()).isEqualTo(1);
    }

    @Test
    @DisplayName("CREATE or UPDATE without device data: VALIDATION_FAILED, PERMANENT, one attempt")
    void givenCreateWithoutDevice_whenRecovered_thenValidationFailed() {
        recoverer.accept(record, listenerFailure(new InvalidDeviceInfoException("device is required for CREATE")));

        FailedEvent event = published();
        assertThat(event.getErrorCode()).isEqualTo(ErrorCode.VALIDATION_FAILED);
        assertThat(event.getErrorType()).isEqualTo(ErrorType.PERMANENT);
        assertThat(event.getAttempts()).isEqualTo(1);
    }

    @Test
    @DisplayName("Shard unavailable after all attempts: STORAGE_UNAVAILABLE, TRANSIENT, attempts from the delivery counter")
    void givenShardDownAfterAllAttempts_whenRecovered_thenStorageUnavailableTransient() {
        record.headers().add(KafkaHeaders.DELIVERY_ATTEMPT, ByteBuffer.allocate(Integer.BYTES).putInt(3).array());

        recoverer.accept(record, listenerFailure(new DeviceNotStoredException("dev-1", "shard_1",
                new StorageAccessException(StorageFailureReason.CONNECTION, "shard is down", null))));

        FailedEvent event = published();
        assertThat(event.getErrorCode()).isEqualTo(ErrorCode.STORAGE_UNAVAILABLE);
        assertThat(event.getErrorType()).isEqualTo(ErrorType.TRANSIENT);
        assertThat(event.getAttempts()).isEqualTo(3);
        verify(metrics).failedEventPublished(ErrorCode.STORAGE_UNAVAILABLE, ErrorType.TRANSIENT);
    }

    @Test
    @DisplayName("Unexpected error after all attempts: PROCESSING_FAILED, TRANSIENT, attempts from the delivery counter")
    void givenUnexpectedErrorAfterAllAttempts_whenRecovered_thenProcessingFailedTransient() {
        record.headers().add(KafkaHeaders.DELIVERY_ATTEMPT, ByteBuffer.allocate(Integer.BYTES).putInt(3).array());

        recoverer.accept(record, listenerFailure(new IllegalStateException("bug")));

        FailedEvent event = published();
        assertThat(event.getErrorCode()).isEqualTo(ErrorCode.PROCESSING_FAILED);
        assertThat(event.getErrorType()).isEqualTo(ErrorType.TRANSIENT);
        assertThat(event.getAttempts()).isEqualTo(3);
    }

    @Test
    @DisplayName("Not deserializable: DESERIALIZATION_FAILED, PERMANENT, original value bytes from the exception")
    void givenNotDeserializable_whenRecovered_thenDeserializationFailedWithOriginalBytes() {
        byte[] garbage = {1, 2, 3};
        record = new ConsumerRecord<>("devices", 2, 10L, "dev-1", null);
        record.headers().add("x-raw-key", "dev-1".getBytes(StandardCharsets.UTF_8));

        recoverer.accept(record, new ListenerExecutionFailedException("listener failed", new DeserializationException(
                "not avro", garbage, false, new SerializationException("Unknown magic byte!"))));

        FailedEvent event = published();
        assertThat(event.getErrorCode()).isEqualTo(ErrorCode.DESERIALIZATION_FAILED);
        assertThat(event.getErrorType()).isEqualTo(ErrorType.PERMANENT);
        assertThat(event.getAttempts()).isEqualTo(1);
        assertThat(event.getValue()).isEqualTo(ByteBuffer.wrap(garbage));
        assertThat(event.getKey()).isEqualTo(ByteBuffer.wrap("dev-1".getBytes(StandardCharsets.UTF_8)));
        assertThat(event.getDeviceId()).isNull();
    }

    @Test
    @DisplayName("Raw bytes from the wrapper headers are kept as is; service and wrapper headers are not copied")
    void givenRawHeaders_whenRecovered_thenBytesOriginalAndHeadersFiltered() {
        byte[] rawValue = {9, 8, 7, 6};
        record.headers().add("x-raw-key", "dev-1".getBytes(StandardCharsets.UTF_8));
        record.headers().add("x-raw-value", rawValue);
        record.headers().add("traceparent", SPAN_PARENT.getBytes(StandardCharsets.US_ASCII));
        record.headers().add("springDeserializerExceptionValue", new byte[]{1});
        record.headers().add("kafka_deliveryAttempt", new byte[]{0, 0, 0, 1});
        record.headers().add("app-header", new byte[]{5});

        recoverer.accept(record, listenerFailure(new InvalidDeviceInfoException("deviceId is blank")));

        FailedEvent event = published();
        assertThat(event.getValue()).isEqualTo(ByteBuffer.wrap(rawValue));
        assertThat(event.getKey()).isEqualTo(ByteBuffer.wrap("dev-1".getBytes(StandardCharsets.UTF_8)));
        assertThat(event.getHeaders()).containsOnlyKeys("traceparent", "app-header");
        assertThat(event.getHeaders().get("app-header")).isEqualTo(ByteBuffer.wrap(new byte[]{5}));
    }

    @Test
    @DisplayName("Trace from the current span when there is one")
    void givenCurrentSpan_whenRecovered_thenTraceIdFromSpan() {
        Span span = mock(Span.class);
        TraceContext context = mock(TraceContext.class);
        when(tracer.currentSpan()).thenReturn(span);
        when(span.context()).thenReturn(context);
        when(context.traceId()).thenReturn(SPAN_TRACE_ID);
        record.headers().add("traceparent", SPAN_PARENT.getBytes(StandardCharsets.US_ASCII));

        recoverer.accept(record, listenerFailure(new InvalidDeviceInfoException("deviceId is blank")));

        assertThat(published().getTraceId()).isEqualTo(SPAN_TRACE_ID);
    }

    @Test
    @DisplayName("No current span: trace from the traceparent header")
    void givenTraceparentWithoutSpan_whenRecovered_thenTraceIdFromHeader() {
        record.headers().add("traceparent", SPAN_PARENT.getBytes(StandardCharsets.US_ASCII));

        recoverer.accept(record, listenerFailure(new InvalidDeviceInfoException("deviceId is blank")));

        assertThat(published().getTraceId()).isEqualTo(TRACEPARENT_TRACE_ID);
    }

    @Test
    @DisplayName("No trace at all: traceId is null")
    void givenNoTrace_whenRecovered_thenTraceIdNull() {
        recoverer.accept(record, listenerFailure(new InvalidDeviceInfoException("deviceId is blank")));

        assertThat(published().getTraceId()).isNull();
    }

    @Test
    @DisplayName("Publication fails: exception (offset not acknowledged), no metric")
    void givenPublishFails_whenRecovered_thenExceptionAndNoMetric() {
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        assertThatThrownBy(() -> recoverer.accept(record, listenerFailure(new InvalidDeviceInfoException("bad"))))
                .isInstanceOf(IllegalStateException.class);

        verify(metrics, never()).failedEventPublished(any(), any());
    }

    @Test
    @DisplayName("Headers as a list: repeated names and a null value kept in source order, service headers left out")
    void givenRepeatedAndNullableHeaders_whenRecovered_thenHeaderListKeepsOrderAndNulls() {
        record.headers().add("tag", "a".getBytes(StandardCharsets.UTF_8));
        record.headers().add("springDeserializerExceptionValue", new byte[]{5});
        record.headers().add("tag", "b".getBytes(StandardCharsets.UTF_8));
        record.headers().add("nullable", null);
        record.headers().add("traceparent", SPAN_PARENT.getBytes(StandardCharsets.US_ASCII));
        record.headers().add(KafkaHeaders.DELIVERY_ATTEMPT, new byte[]{0, 0, 0, 1});

        recoverer.accept(record, listenerFailure(new InvalidDeviceInfoException("deviceId is blank")));

        FailedEvent event = published();
        assertThat(event.getHeaderList()).extracting("name")
                .containsExactly("tag", "tag", "nullable", "traceparent");
        assertThat(event.getHeaderList()).extracting("value").containsExactly(
                ByteBuffer.wrap("a".getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap("b".getBytes(StandardCharsets.UTF_8)),
                null,
                ByteBuffer.wrap(SPAN_PARENT.getBytes(StandardCharsets.US_ASCII)));
        assertThat(event.getHeaders()).containsOnlyKeys("tag", "traceparent");
        assertThat(event.getHeaders().get("tag")).isEqualTo(ByteBuffer.wrap("b".getBytes(StandardCharsets.UTF_8)));
    }

    private static ListenerExecutionFailedException listenerFailure(Exception cause) {
        return new ListenerExecutionFailedException("listener failed", cause);
    }

    private FailedEvent published() {
        return (FailedEvent) sent().value();
    }

    private String sentKey() {
        return sent().key().toString();
    }

    private ProducerRecord<Object, Object> sent() {
        ArgumentCaptor<ProducerRecord<Object, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo(FAILED);
        return captor.getValue();
    }
}
