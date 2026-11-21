package com.github.gluhov.iotplatform.devicecollector.config;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.github.gluhov.iotplatform.devicecollector.adapter.out.kafka.FailedEventRecoverer;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.DeviceNotStoredException;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.InvalidDeviceInfoException;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.StorageAccessException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.FailureReason;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StorageFailureReason;
import com.github.gluhov.iotplatform.devicecollector.support.LogCapture;
import com.github.gluhov.iotplatform.devicecollector.support.MetricsFacade;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import com.github.gluhov.iotplatform.events.avro.ErrorCode;
import com.github.gluhov.iotplatform.events.avro.ErrorType;
import com.github.gluhov.iotplatform.events.avro.FailedEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.health.contributor.Status;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.BackOffExecution;

import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.github.gluhov.iotplatform.devicecollector.data.TestData.T0;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.info;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Политика ошибок приёма: что уходит в failed-events сразу, что после повторов, что не уходит никогда,
 * и что offset подтверждается только после записи в failed-events. Обработчик ошибок - тот же бин, что в сервисе,
 * Kafka заменена моками: записи в failed-events видны через KafkaTemplate, commit и seek - через Consumer.
 */
class KafkaConfigTest {
    private static final String FAILED = "failed-events";
    private static final TopicPartition PARTITION = new TopicPartition("devices", 2);

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<Object, Object> template = mock(KafkaTemplate.class);
    private final Tracer tracer = mock(Tracer.class);
    @SuppressWarnings("unchecked")
    private final Consumer<Object, Object> consumer = mock(Consumer.class);
    private final MessageListenerContainer container = mock(MessageListenerContainer.class);
    private final ConsumerRecord<Object, Object> record = new ConsumerRecord<>("devices", 2, 10L, "dev-1",
            info("dev-1", "evt-1", DeviceUpsertEventType.CREATED, T0).build());
    private final DevicesConsumerOffsets offsets = new DevicesConsumerOffsets(() -> {
        throw new UnsupportedOperationException("Admin is not used by the error handler");
    }, "devices", "device-collector");
    private final MetricsFacade metrics = new MetricsFacade(registry);
    private DefaultErrorHandler handler;

    @BeforeEach
    void setUp() {
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        ContainerProperties properties = new ContainerProperties("devices");
        properties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        when(container.getContainerProperties()).thenReturn(properties);
        handler = errorHandler(3);
    }

    // Обработчик ошибок с заданным числом попыток; остальные настройки - как в сервисе
    private DefaultErrorHandler errorHandler(int maxAttempts) {
        FailedEventRecoverer recoverer = new FailedEventRecoverer(template, tracer, metrics,
                Clock.fixed(Instant.parse("2026-11-21T10:00:00Z"), ZoneOffset.UTC),
                new KafkaProperties("devices", FAILED, Duration.ofSeconds(15)));
        return new KafkaConfig().kafkaErrorHandler(recoverer,
                new ProcessingProperties(maxAttempts, Duration.ofMillis(1), 2.0, Duration.ofDays(1), Duration.ofMinutes(5)),
                metrics, offsets);
    }

    @Test
    @DisplayName("Invalid message: published to failed-events at once (same partition), then its offset is committed")
    void givenInvalidMessage_whenHandled_thenPublishedWithoutRetries() {
        try (LogCapture logs = LogCapture.of(FailedEventRecoverer.class)) {
            handle(new ListenerExecutionFailedException("listener failed", new InvalidDeviceInfoException("deviceId is blank")));

            ILoggingEvent event = logs.singleStartingWith(Level.ERROR, "Message moved to failed-events (VALIDATION_FAILED)");
            assertThat(event.getMDCPropertyMap()).containsEntry("status", "FAILED_EVENT").containsEntry("shard", "unknown")
                    .containsEntry("deviceId", "dev-1").containsEntry("offset", "10")
                    .containsEntry("errorClass", InvalidDeviceInfoException.class.getName());
        }
        FailedEvent failed = sentToFailedEvents();
        assertThat(failed.getErrorCode()).isEqualTo(ErrorCode.VALIDATION_FAILED);
        assertThat(failed.getAttempts()).isEqualTo(1);
        verify(consumer).commitSync(Map.of(PARTITION, new OffsetAndMetadata(11L)), null);
        assertThat(publishedCount(ErrorCode.VALIDATION_FAILED, ErrorType.PERMANENT)).isEqualTo(1);
    }

    @Test
    @DisplayName("Database error: retried (seek back) until max-attempts, then failed-events and commit")
    void givenStorageFailure_whenHandled_thenRetriedThenPublished() {
        Exception failure = new ListenerExecutionFailedException("listener failed", new DeviceNotStoredException("dev-1",
                "shard_1", new StorageAccessException(StorageFailureReason.CONNECTION, "shard is down", null)));

        handleAndExpectRetry(failure);
        handleAndExpectRetry(failure);
        verify(template, never()).send(any(ProducerRecord.class));
        verify(consumer, times(2)).seek(PARTITION, 10L);

        record.headers().add(KafkaHeaders.DELIVERY_ATTEMPT, ByteBuffer.allocate(Integer.BYTES).putInt(3).array());
        try (LogCapture logs = LogCapture.of(FailedEventRecoverer.class)) {
            handle(failure);
            assertThat(logs.singleStartingWith(Level.ERROR, "Message moved to failed-events (STORAGE_UNAVAILABLE)")
                    .getMDCPropertyMap()).containsEntry("shard", "shard_1");
        }
        FailedEvent failed = sentToFailedEvents();
        assertThat(failed.getErrorCode()).isEqualTo(ErrorCode.STORAGE_UNAVAILABLE);
        assertThat(failed.getErrorType()).isEqualTo(ErrorType.TRANSIENT);
        assertThat(failed.getAttempts()).isEqualTo(3);
        verify(consumer).commitSync(Map.of(PARTITION, new OffsetAndMetadata(11L)), null);
        assertThat(publishedCount(ErrorCode.STORAGE_UNAVAILABLE, ErrorType.TRANSIENT)).isEqualTo(1);
        // Сообщение завершилось ошибкой один раз; получение посчитал обработчик (первая доставка), не failed-events
        assertThat(counter("devices.messages.failed", FailureReason.STORAGE)).isEqualTo(1);
        assertThat(registry.get("devices.messages.received").counter().count()).isZero();
    }

    @Test
    @DisplayName("One attempt: the first storage error goes to failed-events at once, no retry")
    void givenOneAttempt_whenStorageFailure_thenPublishedAtOnce() {
        handler = errorHandler(1);

        handle(new ListenerExecutionFailedException("listener failed", new DeviceNotStoredException("dev-1",
                "shard_1", new StorageAccessException(StorageFailureReason.CONNECTION, "shard is down", null))));

        assertThat(sentToFailedEvents().getErrorCode()).isEqualTo(ErrorCode.STORAGE_UNAVAILABLE);
        verify(consumer, never()).seek(any(TopicPartition.class), anyLong());
        verify(consumer).commitSync(Map.of(PARTITION, new OffsetAndMetadata(11L)), null);
    }

    @Test
    @DisplayName("failed-events unavailable: offset not committed, the message is not counted as failed")
    void givenPublishFailure_whenHandled_thenNotCommittedNotCounted() {
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("topic down")));

        // Восстановление не удалось: обработчик возвращает consumer на offset сообщения (исключение - деталь Spring)
        catchThrowable(() -> handle(new ListenerExecutionFailedException("listener failed",
                new InvalidDeviceInfoException("deviceId is blank"))));

        verify(consumer, never()).commitSync(anyMap(), any());
        assertThat(counter("devices.messages.failed", FailureReason.INVALID)).isZero();
        assertThat(publishedCount(ErrorCode.VALIDATION_FAILED, ErrorType.PERMANENT)).isZero();
    }

    @Test
    @DisplayName("Not deserializable: received and failed counted once, in the error handler; bytes published as is")
    void givenPoisonPill_whenHandled_thenReceivedAndFailedOnce() {
        byte[] garbage = {1, 2, 3};

        handle(new DeserializationException("not avro", garbage, false, new SerializationException("Unknown magic byte!")));

        assertThat(registry.get("devices.messages.received").counter().count()).isEqualTo(1);
        assertThat(counter("devices.messages.failed", FailureReason.DESERIALIZATION)).isEqualTo(1);
        assertThat(counter("devices.processing.errors", FailureReason.DESERIALIZATION)).isEqualTo(1);
        FailedEvent failed = sentToFailedEvents();
        assertThat(failed.getErrorCode()).isEqualTo(ErrorCode.DESERIALIZATION_FAILED);
        assertThat(failed.getValue()).isEqualTo(ByteBuffer.wrap(garbage));
    }

    @Test
    @DisplayName("Production retry policy: 3 attempts in total, pauses 1 s and 2 s, then failed-events")
    void givenApplicationDefaults_whenBackOff_thenOneAndTwoSecondsThenStop() {
        BackOffExecution execution = KafkaConfig.storageBackOff(
                new ProcessingProperties(3, Duration.ofSeconds(1), 2.0, Duration.ofDays(1), Duration.ofMinutes(5))).start();

        assertThat(execution.nextBackOff()).isEqualTo(1000L);
        assertThat(execution.nextBackOff()).isEqualTo(2000L);
        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }

    @Test
    @DisplayName("One attempt in settings: no retries, straight to failed-events")
    void givenOneAttempt_whenBackOff_thenStopAtOnce() {
        BackOffExecution execution = KafkaConfig.storageBackOff(
                new ProcessingProperties(1, Duration.ofSeconds(1), 2.0, Duration.ofDays(1), Duration.ofMinutes(5))).start();

        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }

    @Test
    @DisplayName("Schema Registry unavailable: retried without limit, never published to failed-events")
    void givenTransientDeserializationFailure_whenHandled_thenRetriedWithoutPublishing() {
        Exception failure = new DeserializationException("schema registry timeout", new byte[]{0, 0, 0, 0, 1}, false,
                new SerializationException("Error retrieving Avro schema", new SocketTimeoutException("Read timed out")));

        for (int attempt = 0; attempt < 5; attempt++) {
            handleAndExpectRetry(failure);
        }

        verify(template, never()).send(any(ProducerRecord.class));
        verify(consumer, times(5)).seek(PARTITION, 10L);
        assertThat(counter("devices.messages.failed", FailureReason.DESERIALIZATION)).isZero();
    }

    @Test
    @DisplayName("Failure reason: deserialization, invalid, storage, unexpected error")
    void givenExceptions_whenReasonOf_thenClassified() {
        assertThat(KafkaConfig.reasonOf(new DeserializationException("not avro", new byte[0], false,
                new SerializationException("Unknown magic byte!")))).isEqualTo(FailureReason.DESERIALIZATION);
        assertThat(KafkaConfig.reasonOf(new ListenerExecutionFailedException("x", new InvalidDeviceInfoException("bad"))))
                .isEqualTo(FailureReason.INVALID);
        assertThat(KafkaConfig.reasonOf(new ListenerExecutionFailedException("x", new IllegalStateException("bug"))))
                .isEqualTo(FailureReason.ERROR);
    }

    @Test
    @DisplayName("Committed offset out of range: listener stopped abnormally, health DOWN, not a silent reset (ADR-0011)")
    void givenOffsetOutOfRange_whenOtherException_thenContainerStoppedAndHealthDown() {
        OffsetOutOfRangeException outOfRange = new OffsetOutOfRangeException("out of range", Map.of(PARTITION, 5L));

        Throwable thrown = catchThrowable(() -> handler.handleOtherException(outOfRange, consumer, container, false));

        assertThat(thrown).isInstanceOf(KafkaException.class).hasCause(outOfRange);
        verify(container, timeout(5_000)).stopAbnormally(any());
        assertThat(offsets.health().getStatus()).isEqualTo(Status.DOWN);
        assertThat(offsets.health().getDetails()).containsEntry("partitions", "{devices-2=5}");
    }

    @Test
    @DisplayName("Other consumer exception: handled as before, the listener is not stopped")
    void givenOtherConsumerException_whenOtherException_thenNotStopped() {
        Throwable thrown = catchThrowable(() -> handler.handleOtherException(new IllegalStateException("commit failed"),
                consumer, container, false));

        assertThat(thrown).isInstanceOf(IllegalStateException.class);
        verify(container, never()).stopAbnormally(any());
        assertThat(offsets.health().getStatus()).isEqualTo(Status.UP);
    }

    private void handle(Exception failure) {
        handler.handleRemaining(failure, List.of(record), consumer, container);
    }

    // Запись не восстановлена: обработчик вернул consumer на её offset и сообщает контейнеру о повторе
    // (RecordInRetryException - внутренний класс Spring Kafka, поэтому проверка по имени)
    private void handleAndExpectRetry(Exception failure) {
        assertThatThrownBy(() -> handle(failure))
                .satisfies(e -> assertThat(e.getClass().getSimpleName()).isEqualTo("RecordInRetryException"))
                .hasCause(failure);
    }

    private FailedEvent sentToFailedEvents() {
        ArgumentCaptor<ProducerRecord<Object, Object>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(sent.capture());
        assertThat(sent.getValue().topic()).isEqualTo(FAILED);
        assertThat(sent.getValue().key()).isEqualTo("devices-2-10");
        return (FailedEvent) sent.getValue().value();
    }

    private double counter(String name, FailureReason reason) {
        return registry.get(name).tag("reason", reason.label()).counter().count();
    }

    private double publishedCount(ErrorCode code, ErrorType type) {
        return registry.get("failed.events.published")
                .tag("error_code", code.name())
                .tag("error_type", type.name()).counter().count();
    }
}
