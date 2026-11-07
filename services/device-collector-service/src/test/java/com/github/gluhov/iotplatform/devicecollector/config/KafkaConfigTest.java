package com.github.gluhov.iotplatform.devicecollector.config;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.DeviceNotStoredException;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.InvalidDeviceInfoException;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.StorageAccessException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.FailureReason;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StorageFailureReason;
import com.github.gluhov.iotplatform.devicecollector.support.LogCapture;
import com.github.gluhov.iotplatform.devicecollector.support.MetricsFacade;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
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

import static com.github.gluhov.iotplatform.devicecollector.data.TestData.T0;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.info;
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
 * Политика ошибок приёма: что уходит в devices-dlt сразу, что после повторов, что не уходит никогда,
 * и что offset подтверждается только после записи в devices-dlt. Обработчик ошибок - тот же бин, что в сервисе,
 * Kafka заменена моками: записи в devices-dlt видны через KafkaTemplate, commit и seek - через Consumer.
 */
class KafkaConfigTest {
    private static final String DLT = "devices-dlt";
    private static final TopicPartition PARTITION = new TopicPartition("devices", 2);

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<Object, Object> template = mock(KafkaTemplate.class);
    @SuppressWarnings("unchecked")
    private final Consumer<Object, Object> consumer = mock(Consumer.class);
    private final MessageListenerContainer container = mock(MessageListenerContainer.class);
    private final ConsumerRecord<Object, Object> record = new ConsumerRecord<>("devices", 2, 10L, "dev-1",
            info("dev-1", "evt-1", DeviceUpsertEventType.CREATED, T0).build());
    private DefaultErrorHandler handler;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        ProducerFactory<Object, Object> producerFactory = mock(ProducerFactory.class);
        when(producerFactory.copyWithConfigurationOverride(anyMap())).thenReturn(mock(ProducerFactory.class));
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        ContainerProperties properties = new ContainerProperties("devices");
        properties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        when(container.getContainerProperties()).thenReturn(properties);
        handler = new KafkaConfig().kafkaErrorHandler(producerFactory, template, new KafkaProperties("devices", DLT),
                new ProcessingProperties(3, Duration.ofMillis(1), 2.0, Duration.ofDays(1), Duration.ofMinutes(5)),
                new MetricsFacade(registry));
    }

    @Test
    @DisplayName("Invalid message: moved to devices-dlt at once (same partition), then its offset is committed")
    void givenInvalidMessage_whenHandled_thenDeadLetteredWithoutRetries() {
        try (LogCapture logs = LogCapture.of(KafkaConfig.class)) {
            handle(new ListenerExecutionFailedException("listener failed", new InvalidDeviceInfoException("deviceId is blank")));

            ILoggingEvent event = logs.singleStartingWith(Level.ERROR, "Message moved to devices-dlt (invalid)");
            assertThat(event.getMDCPropertyMap()).containsEntry("status", "DLT").containsEntry("shard", "unknown")
                    .containsEntry("deviceId", "dev-1").containsEntry("offset", "10")
                    .containsEntry("errorClass", InvalidDeviceInfoException.class.getName());
        }
        ProducerRecord<Object, Object> dead = sentToDlt();
        assertThat(dead.partition()).isEqualTo(2);
        assertThat(dead.key()).isEqualTo("dev-1");
        assertThat(dead.value()).isEqualTo(record.value());
        verify(consumer).commitSync(Map.of(PARTITION, new OffsetAndMetadata(11L)), null);
        assertThat(dlt(FailureReason.INVALID)).isEqualTo(1);
    }

    @Test
    @DisplayName("Database error: retried (seek back) until max-attempts, then devices-dlt and commit")
    void givenStorageFailure_whenHandled_thenRetriedThenDeadLettered() {
        Exception failure = new ListenerExecutionFailedException("listener failed", new DeviceNotStoredException("dev-1",
                "shard_1", new StorageAccessException(StorageFailureReason.CONNECTION, "shard is down", null)));

        handleAndExpectRetry(failure);
        handleAndExpectRetry(failure);
        verify(template, never()).send(any(ProducerRecord.class));
        verify(consumer, times(2)).seek(PARTITION, 10L);

        try (LogCapture logs = LogCapture.of(KafkaConfig.class)) {
            handle(failure);
            assertThat(logs.singleStartingWith(Level.ERROR, "Message moved to devices-dlt (storage)").getMDCPropertyMap())
                    .containsEntry("shard", "shard_1");
        }
        assertThat(sentToDlt().topic()).isEqualTo(DLT);
        verify(consumer).commitSync(Map.of(PARTITION, new OffsetAndMetadata(11L)), null);
        assertThat(dlt(FailureReason.STORAGE)).isEqualTo(1);
        // Сообщение завершилось ошибкой один раз; получение посчитал обработчик (первая доставка), не DLT
        assertThat(counter("devices.messages.failed", FailureReason.STORAGE)).isEqualTo(1);
        assertThat(registry.get("devices.messages.received").counter().count()).isZero();
    }

    @Test
    @DisplayName("devices-dlt unavailable: offset not committed, the message is not counted as failed")
    void givenDeadLetterPublishFailure_whenHandled_thenNotCommittedNotCounted() {
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("dlt down")));

        // Восстановление не удалось: обработчик возвращает consumer на offset сообщения (исключение - деталь Spring)
        catchThrowable(() -> handle(new ListenerExecutionFailedException("listener failed",
                new InvalidDeviceInfoException("deviceId is blank"))));

        verify(consumer, never()).commitSync(anyMap(), any());
        assertThat(counter("devices.messages.failed", FailureReason.INVALID)).isZero();
        assertThat(dlt(FailureReason.INVALID)).isZero();
    }

    @Test
    @DisplayName("Not deserializable: received and failed counted once, in the error handler")
    void givenPoisonPill_whenHandled_thenReceivedAndFailedOnce() {
        handle(new DeserializationException("not avro", new byte[]{1, 2, 3}, false,
                new SerializationException("Unknown magic byte!")));

        assertThat(registry.get("devices.messages.received").counter().count()).isEqualTo(1);
        assertThat(counter("devices.messages.failed", FailureReason.DESERIALIZATION)).isEqualTo(1);
        assertThat(counter("devices.processing.errors", FailureReason.DESERIALIZATION)).isEqualTo(1);
    }

    @Test
    @DisplayName("Production retry policy: 3 attempts in total, pauses 1 s and 2 s, then devices-dlt")
    void givenApplicationDefaults_whenBackOff_thenOneAndTwoSecondsThenStop() {
        BackOffExecution execution = KafkaConfig.storageBackOff(
                new ProcessingProperties(3, Duration.ofSeconds(1), 2.0, Duration.ofDays(1), Duration.ofMinutes(5))).start();

        assertThat(execution.nextBackOff()).isEqualTo(1000L);
        assertThat(execution.nextBackOff()).isEqualTo(2000L);
        assertThat(execution.nextBackOff()).isEqualTo(BackOffExecution.STOP);
    }

    @Test
    @DisplayName("Schema Registry unavailable: retried without limit, never a dead letter")
    void givenTransientDeserializationFailure_whenHandled_thenRetriedWithoutDeadLetter() {
        Exception failure = new DeserializationException("schema registry timeout", new byte[]{0, 0, 0, 0, 1}, false,
                new SerializationException("Error retrieving Avro schema", new SocketTimeoutException("Read timed out")));

        for (int attempt = 0; attempt < 5; attempt++) {
            handleAndExpectRetry(failure);
        }

        verify(template, never()).send(any(ProducerRecord.class));
        verify(consumer, times(5)).seek(PARTITION, 10L);
        assertThat(dlt(FailureReason.DESERIALIZATION)).isZero();
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
    private ProducerRecord<Object, Object> sentToDlt() {
        ArgumentCaptor<ProducerRecord<Object, Object>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(sent.capture());
        assertThat(sent.getValue().topic()).isEqualTo(DLT);
        return sent.getValue();
    }

    private double counter(String name, FailureReason reason) {
        return registry.get(name).tag("reason", reason.label()).counter().count();
    }

    private double dlt(FailureReason reason) {
        return registry.get("devices.dlt.published").tag("reason", reason.label()).counter().count();
    }
}