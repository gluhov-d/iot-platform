package com.github.gluhov.iotplatform.failedeventsprocessor.config;

import com.github.gluhov.iotplatform.failedeventsprocessor.adapter.in.kafka.KafkaErrorClassifier;
import com.github.gluhov.iotplatform.failedeventsprocessor.adapter.in.kafka.UnreadableRecorder;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveDeniedException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveUnavailableException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerPausingBackOffHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Повторы без предела: архив недоступен - сообщение не подтверждается и повторяется с нарастающим интервалом
 * (FAILED_EVENTS_RETRY_*). Контейнер на время паузы останавливает потребление, группа остаётся Stable (R6).
 */
@Configuration
public class KafkaConfig {

    /** Без повторов: запись сразу уходит в восстановитель (unreadable). */
    static final BackOff NO_RETRIES = new FixedBackOff(0L, 0L);

    @Bean
    ListenerContainerPauseService listenerContainerPauseService(ListenerContainerRegistry registry) {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("failed-events-pause-");
        scheduler.initialize();
        return new ListenerContainerPauseService(registry, scheduler);
    }

    /**
     * Контейнер передаёт слушателю только настоящий null: ошибку разбора (с исходными байтами) он выбрасывает сам
     * (checkDeserExWhenValueNull), и обработчик ошибок решает про повтор или unreadable.
     */
    @Bean
    ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> deserializationChecks() {
        return container -> {
            container.getContainerProperties().setCheckDeserExWhenKeyNull(true);
            container.getContainerProperties().setCheckDeserExWhenValueNull(true);
        };
    }

    @Bean
    DefaultErrorHandler failedEventsErrorHandler(FailedEventsS3Properties properties,
                                                 ListenerContainerPauseService pauseService,
                                                 UnreadableRecorder unreadableRecorder) {
        FailedEventsS3Properties.Retry retry = properties.retry();
        ExponentialBackOff unlimited = new ExponentialBackOff(retry.initialInterval().toMillis(), retry.multiplier());
        unlimited.setMaxInterval(retry.maxInterval().toMillis());
        unlimited.setMaxElapsedTime(Long.MAX_VALUE);
        // Повторы без предела: архив недоступен или отказал, Schema Registry временно недоступен
        DefaultErrorHandler handler = new DefaultErrorHandler(pacedRecoverer(unreadableRecorder, unlimited), unlimited,
                new ContainerPausingBackOffHandler(pauseService));
        // Ошибку разбора по умолчанию не повторяют: решает backOffFor (временная - повтор, остальное - unreadable)
        handler.removeClassification(DeserializationException.class);
        handler.setBackOffFunction((record, exception) -> backOffFor(exception, unlimited));
        // Смещение подтверждается после записи unreadable: восстановленная запись считается обработанной
        handler.setCommitRecovered(true);
        return handler;
    }

    /**
     * Решение о повторе. Нечитаемая запись без временной причины - {@link #NO_RETRIES}: сразу в восстановитель
     * (объект unreadable). Временная ошибка Schema Registry и любой отказ архива - без предела (R7).
     */
    static BackOff backOffFor(Throwable exception, BackOff unlimited) {
        if (KafkaErrorClassifier.isUnreadable(exception) && !KafkaErrorClassifier.isTransient(exception)) {
            return NO_RETRIES;
        }
        return unlimited;
    }

    /**
     * Восстановитель без повторов: если запись unreadable не записалась в архив, исключение уходит в контейнер, и он
     * переотправляет запись сразу - горячий цикл. Поэтому каждый сбой записи ждёт нарастающий интервал
     * (FAILED_EVENTS_RETRY_*), после успешной записи интервал сбрасывается.
     */
    static ConsumerRecordRecoverer pacedRecoverer(ConsumerRecordRecoverer recorder, BackOff backOff) {
        AtomicReference<BackOffExecution> execution = new AtomicReference<>();
        return (record, exception) -> {
            try {
                recorder.accept(record, exception);
                execution.set(null);
            } catch (ArchiveUnavailableException | ArchiveDeniedException e) {
                BackOffExecution current = execution.updateAndGet(
                        previous -> previous == null ? backOff.start() : previous);
                pause(current.nextBackOff());
                throw e;
            }
        };
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
