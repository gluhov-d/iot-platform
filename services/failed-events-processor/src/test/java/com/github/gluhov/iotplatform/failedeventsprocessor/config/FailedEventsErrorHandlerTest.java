package com.github.gluhov.iotplatform.failedeventsprocessor.config;

import com.github.gluhov.iotplatform.failedeventsprocessor.adapter.in.kafka.UnreadableRecorder;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveUnavailableException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.PutOutcome;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.service.ArchiveService;
import com.github.gluhov.iotplatform.failedeventsprocessor.support.MetricsFacade;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.net.SocketTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;

/**
 * Повторы записи, которую не удаётся записать в архив как unreadable (R6, R7): восстановитель падает, обработчик
 * ошибок не должен повторять её без паузы (горячий цикл). Паузы - FAILED_EVENTS_RETRY_*.
 */
class FailedEventsErrorHandlerTest {

    @Test
    void givenUnreadableRecordAndArchiveDown_whenRecovererFailsRepeatedly_thenRetriesArePaused() {
        AtomicInteger recovererCalls = new AtomicInteger();
        UnreadableRecorder recorder = new UnreadableRecorder(new ArchiveService((key, body, metadata) -> {
            recovererCalls.incrementAndGet();
            throw new ArchiveUnavailableException("timeout", null);
        }), new MetricsFacade(new SimpleMeterRegistry()));
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.initialize();
        ListenerContainerPauseService pauseService = new ListenerContainerPauseService(
                mock(ListenerContainerRegistry.class), scheduler);
        FailedEventsS3Properties properties = new FailedEventsS3Properties("failed-events", null,
                new FailedEventsS3Properties.Retry(Duration.ofMillis(100), Duration.ofSeconds(10), 2.0));
        DefaultErrorHandler handler = new KafkaConfig().failedEventsErrorHandler(properties, pauseService, recorder);

        MessageListenerContainer container = mock(MessageListenerContainer.class);
        Consumer<?, ?> consumer = mock(Consumer.class);
        ConsumerRecord<String, Object> record = new ConsumerRecord<>("failed-events", 1, 8L,
                Instant.parse("2026-11-21T10:20:00Z").toEpochMilli(), TimestampType.CREATE_TIME, 0, 3, null, null,
                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());
        DeserializationException failure = new DeserializationException("не разобрано", new byte[]{1, 2, 3}, false,
                new SerializationException("Unknown magic byte!"));

        // Контейнер переотправляет запись после каждого false: без паузы это горячий цикл
        long started = System.nanoTime();
        List<String> outcomes = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            outcomes.add("handled=" + handler.handleOne(failure, record, consumer, container));
        }
        long elapsedMs = Duration.ofNanos(System.nanoTime() - started).toMillis();
        scheduler.shutdown();

        assertThat(recovererCalls.get()).as("попытки записи unreadable: %s", outcomes).isEqualTo(4);
        assertThat(elapsedMs)
                .as("четыре неудачные попытки (100+200+400+800 мс по FAILED_EVENTS_RETRY_*) без паузы: %s, %s",
                        outcomes, mockingDetails(container).getInvocations())
                .isGreaterThanOrEqualTo(1_000L);
    }

    /** Временный сбой Schema Registry, затем 404 той же записи: восстановитель вызван, повторов больше нет (R28). */
    @Test
    void givenTransientFailureThenNotFound_whenSameRecordFails_thenRecovererCalledAndNoMoreRetries() {
        Fixture fixture = new Fixture();

        boolean firstHandled = fixture.handler.handleOne(schemaRegistryTimeout(), fixture.record,
                fixture.consumer, fixture.container);
        boolean secondHandled = fixture.handler.handleOne(schemaRegistryNotFound(), fixture.record,
                fixture.consumer, fixture.container);

        assertThat(firstHandled).as("после временного сбоя запись в повторах").isFalse();
        assertThat(secondHandled).as("после 404 запись уходит в восстановитель").isTrue();
        assertThat(fixture.archiveCalls.get()).as("восстановитель вызван после 404").isEqualTo(1);
        fixture.close();
    }

    /** Временный сбой, повторённый подряд, остаётся в повторах: восстановитель не вызван. */
    @Test
    void givenRepeatedTransientFailures_whenHandled_thenRecordStaysInRetry() {
        Fixture fixture = new Fixture();

        List<Boolean> handled = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            handled.add(fixture.handler.handleOne(schemaRegistryTimeout(), fixture.record,
                    fixture.consumer, fixture.container));
        }

        assertThat(handled).as("все сбои - повтор").containsOnly(false);
        assertThat(fixture.archiveCalls.get()).as("восстановитель не вызван").isZero();
        fixture.close();
    }

    /** Первый же сбой - 404 схемы: восстановитель вызывается сразу. */
    @Test
    void givenNotFoundOnFirstFailure_whenHandled_thenRecovererCalledImmediately() {
        Fixture fixture = new Fixture();

        boolean handled = fixture.handler.handleOne(schemaRegistryNotFound(), fixture.record,
                fixture.consumer, fixture.container);

        assertThat(handled).isTrue();
        assertThat(fixture.archiveCalls.get()).isEqualTo(1);
        fixture.close();
    }

    private static DeserializationException schemaRegistryTimeout() {
        return new DeserializationException("не разобрано", new byte[]{1, 2, 3}, false,
                new SerializationException("Error deserializing Avro message", new SocketTimeoutException("Read timed out")));
    }

    private static DeserializationException schemaRegistryNotFound() {
        return new DeserializationException("не разобрано", new byte[]{1, 2, 3}, false,
                new SerializationException("Error deserializing Avro message",
                        new RestClientException("Schema 99999 not found", 404, 40403)));
    }

    /** Обработчик, собранный KafkaConfig, и запись с одним и тем же смещением для всех сбоев сценария. */
    private static final class Fixture {
        final AtomicInteger archiveCalls = new AtomicInteger();
        final ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        final DefaultErrorHandler handler;
        final MessageListenerContainer container = mock(MessageListenerContainer.class);
        final Consumer<?, ?> consumer = mock(Consumer.class);
        final ConsumerRecord<String, Object> record = new ConsumerRecord<>("failed-events", 1, 8L,
                Instant.parse("2026-11-21T10:20:00Z").toEpochMilli(), TimestampType.CREATE_TIME, 0, 3, null, null,
                new org.apache.kafka.common.header.internals.RecordHeaders(), Optional.empty());

        Fixture() {
            scheduler.initialize();
            UnreadableRecorder recorder = new UnreadableRecorder(new ArchiveService((key, body, metadata) -> {
                archiveCalls.incrementAndGet();
                return PutOutcome.CREATED;
            }), new MetricsFacade(new SimpleMeterRegistry()));
            ListenerContainerPauseService pauseService = new ListenerContainerPauseService(
                    mock(ListenerContainerRegistry.class), scheduler);
            FailedEventsS3Properties properties = new FailedEventsS3Properties("failed-events", null,
                    new FailedEventsS3Properties.Retry(Duration.ofMillis(100), Duration.ofSeconds(10), 2.0));
            handler = new KafkaConfig().failedEventsErrorHandler(properties, pauseService, recorder);
        }

        void close() {
            scheduler.shutdown();
        }
    }
}
