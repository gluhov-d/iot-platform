package com.github.gluhov.iotplatform.eventscollector.config;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.github.gluhov.iotplatform.events.avro.ErrorCode;
import com.github.gluhov.iotplatform.events.avro.FailedEvent;
import com.github.gluhov.iotplatform.eventscollector.adapter.out.kafka.FailedEventRecoverer;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.EventPersistenceException;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.InvalidEventException;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.CollectorMetrics;
import com.github.gluhov.iotplatform.eventscollector.support.LogCapture;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.BackOffExecution;

import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.github.gluhov.iotplatform.eventscollector.data.TestData.validEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Политика ошибок приёма: что уходит в failed-events сразу, что после повторов, что не уходит никогда,
 * и что offset подтверждается только после записи в failed-events. Обработчик ошибок - тот же бин, что в сервисе,
 * Kafka заменена моками: записи в failed-events видны через KafkaOperations, commit и seek - через Consumer.
 */
class KafkaConfigTest {
    private static final String FAILED_EVENTS = "failed-events";
    private static final TopicPartition PARTITION = new TopicPartition("events", 2);

    @SuppressWarnings("unchecked")
    private final KafkaOperations<String, Object> template = mock(KafkaOperations.class);
    @SuppressWarnings("unchecked")
    private final Consumer<Object, Object> consumer = mock(Consumer.class);
    private final MessageListenerContainer container = mock(MessageListenerContainer.class);
    private final ConsumerRecord<Object, Object> record = new ConsumerRecord<>("events", 2, 10L, "dev-1", validEvent());
    private DefaultErrorHandler handler;

    @BeforeEach
    void setUp() {
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        ContainerProperties properties = new ContainerProperties("events");
        properties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        when(container.getContainerProperties()).thenReturn(properties);
        FailedEventRecoverer recoverer = new FailedEventRecoverer(template, mock(Tracer.class), mock(CollectorMetrics.class),
                FAILED_EVENTS, "events-collector-service", Duration.ofSeconds(15));
        handler = new KafkaConfig().kafkaErrorHandler(recoverer,
                new KafkaProperties("events", "devices", FAILED_EVENTS, Duration.ofSeconds(20)),
                new ProcessingProperties(3, Duration.ofMillis(1), 2.0));
    }

    @Test
    @DisplayName("Invalid event: moved to failed-events at once (same partition), then its offset is committed")
    void givenInvalidEvent_whenHandled_thenFailedEventWithoutRetries() {
        InvalidEventException invalid = new InvalidEventException("eventId is blank");
        try (LogCapture logs = LogCapture.of(KafkaConfig.class)) {
            handle(new ListenerExecutionFailedException("listener failed", invalid));

            ILoggingEvent event = logs.single(Level.ERROR, "Message moved to failed-events (invalid): " + invalid);
            assertThat(event.getMDCPropertyMap()).containsEntry("status", "FAILED_EVENT").containsEntry("reason", "invalid")
                    .containsEntry("offset", "10").containsEntry("partition", "2")
                    .containsEntry("errorClass", InvalidEventException.class.getName());
        }
        ProducerRecord<String, Object> failed = sentToFailedEvents();
        assertThat(failed.key()).isEqualTo("events-2-10");
        FailedEvent event = (FailedEvent) failed.value();
        assertThat(event.getErrorCode()).isEqualTo(ErrorCode.VALIDATION_FAILED);
        assertThat(event.getDeviceId()).isEqualTo("dev-1");
        verify(consumer).commitSync(Map.of(PARTITION, new OffsetAndMetadata(11L)), null);
        verify(consumer, never()).seek(any(), any(Long.class));
    }

    @Test
    @DisplayName("ClickHouse error: retried (seek back) until max-attempts, then failed-events and commit")
    void givenStorageFailure_whenHandled_thenRetriedThenFailedEvent() {
        Exception failure = new ListenerExecutionFailedException("listener failed",
                new EventPersistenceException("insert device_events failed", new IllegalStateException("clickhouse is down")));

        handleAndExpectRetry(failure);
        handleAndExpectRetry(failure);
        verify(template, never()).send(any(ProducerRecord.class));
        verify(consumer, times(2)).seek(PARTITION, 10L);
        verify(consumer, never()).commitSync(anyMap(), any());

        try (LogCapture logs = LogCapture.of(KafkaConfig.class)) {
            handle(failure);
            ILoggingEvent event = logs.single(Level.ERROR, "Message moved to failed-events (storage): java.lang.IllegalStateException: clickhouse is down");
            assertThat(event.getMDCPropertyMap()).containsEntry("reason", "storage");
        }
        FailedEvent event = (FailedEvent) sentToFailedEvents().value();
        assertThat(event.getErrorCode()).isEqualTo(ErrorCode.STORAGE_UNAVAILABLE);
        verify(consumer).commitSync(Map.of(PARTITION, new OffsetAndMetadata(11L)), null);
    }

    @Test
    @DisplayName("failed-events unavailable: offset is not committed, the message will be processed again")
    void givenFailedEventsPublishFailure_whenHandled_thenNotCommitted() {
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("failed-events down")));

        // Восстановление не удалось: обработчик возвращает consumer на offset сообщения (исключение - деталь Spring)
        catchThrowable(() -> handle(new ListenerExecutionFailedException("listener failed",
                new InvalidEventException("eventId is blank"))));

        verify(consumer, never()).commitSync(anyMap(), any());
    }

    @Test
    @DisplayName("Not deserializable: moved to failed-events at once, no retries, offset committed")
    void givenPoisonPill_whenHandled_thenFailedEventWithoutRetries() {
        // Исходные байты в failed-events проверяет FailedEventRecovererTest и интеграционный тест на настоящей Kafka
        handle(new DeserializationException("not avro", new byte[]{1, 2, 3}, false,
                new SerializationException("Unknown magic byte!")));

        FailedEvent event = (FailedEvent) sentToFailedEvents().value();
        assertThat(event.getErrorCode()).isEqualTo(ErrorCode.DESERIALIZATION_FAILED);
        verify(consumer, never()).seek(any(), any(Long.class));
        verify(consumer).commitSync(Map.of(PARTITION, new OffsetAndMetadata(11L)), null);
    }

    @Test
    @DisplayName("Schema Registry unavailable: retried without limit, never a failed event")
    void givenTransientDeserializationFailure_whenHandled_thenRetriedWithoutFailedEvent() {
        Exception failure = new DeserializationException("schema registry timeout", new byte[]{0, 0, 0, 0, 1}, false,
                new SerializationException("Error retrieving Avro schema", new SocketTimeoutException("Read timed out")));

        for (int attempt = 0; attempt < 5; attempt++) {
            handleAndExpectRetry(failure);
        }

        verify(template, never()).send(any(ProducerRecord.class));
        verify(consumer, times(5)).seek(PARTITION, 10L);
    }

    @Test
    @DisplayName("Retry policy from settings: 3 attempts, pauses 1 s and 2 s, then stop")
    void givenThreeAttempts_whenBackOff_thenOneAndTwoSecondsThenStop() {
        BackOffExecution execution = KafkaConfig.storageBackOff(
                new ProcessingProperties(3, Duration.ofSeconds(1), 2.0)).start();

        assertThat(execution.nextBackOff()).isEqualTo(1000L);
        assertThat(execution.nextBackOff()).isEqualTo(2000L);
        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }

    @Test
    @DisplayName("One attempt in settings: no retries, straight to failed-events")
    void givenOneAttempt_whenBackOff_thenStopAtOnce() {
        BackOffExecution execution = KafkaConfig.storageBackOff(
                new ProcessingProperties(1, Duration.ofSeconds(1), 2.0)).start();

        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }

    @Test
    @DisplayName("Settings are validated: attempts < 1, non-positive pause, multiplier < 1 are rejected")
    void givenWrongSettings_whenCreated_thenRejected() {
        assertThatThrownBy(() -> new ProcessingProperties(0, Duration.ofSeconds(1), 2.0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("max-attempts");
        assertThatThrownBy(() -> new ProcessingProperties(3, Duration.ZERO, 2.0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("retry-interval");
        assertThatThrownBy(() -> new ProcessingProperties(3, Duration.ofSeconds(1), 0.5))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("retry-multiplier");
    }

    @Test
    @DisplayName("Reason: deserialization, invalid, storage, error")
    void givenExceptions_whenReasonOf_thenClassified() {
        assertThat(KafkaConfig.reasonOf(new DeserializationException("not avro", new byte[0], false,
                new SerializationException("Unknown magic byte!")))).isEqualTo("deserialization");
        assertThat(KafkaConfig.reasonOf(new ListenerExecutionFailedException("x", new InvalidEventException("bad"))))
                .isEqualTo("invalid");
        assertThat(KafkaConfig.reasonOf(new ListenerExecutionFailedException("x",
                new EventPersistenceException("insert failed", new IllegalStateException("down"))))).isEqualTo("storage");
        assertThat(KafkaConfig.reasonOf(new ListenerExecutionFailedException("x", new IllegalStateException("bug"))))
                .isEqualTo("error");
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

    @SuppressWarnings("unchecked")
    private ProducerRecord<String, Object> sentToFailedEvents() {
        ArgumentCaptor<ProducerRecord<String, Object>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(sent.capture());
        assertThat(sent.getValue().topic()).isEqualTo(FAILED_EVENTS);
        return sent.getValue();
    }
}
