package com.github.gluhov.iotplatform.eventscollector.config;

import com.github.gluhov.iotplatform.eventscollector.domain.exception.EventPersistenceException;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.InvalidEventException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Обработка ошибок приёма из events:
 * <ul>
 *   <li>невалидное или недесериализуемое сообщение - сразу в failed-events;</li>
 *   <li>ошибка записи в ClickHouse и непредвиденная ошибка - app.processing.max-attempts попыток с растущей
 *       паузой, затем failed-events: следующие сообщения раздела продолжают обрабатываться;</li>
 *   <li>Schema Registry недоступен - повторять без ограничения: это сбой не сообщения, а инфраструктуры,
 *       иначе в failed-events ушли бы все сообщения подряд.</li>
 * </ul>
 * Offset сообщения, записанного в failed-events, подтверждается только после успешной публикации;
 * не удалось записать - сообщение не подтверждено и будет обработано снова.
 */
@Configuration(proxyBeanMethods = false)
@Slf4j
public class KafkaConfig {
    private static final long MAX_BACKOFF_MS = 10_000L;
    private static final BackOff NO_RETRIES = new FixedBackOff(0L, 0L);

    // Записывает сообщение в failed-events: бин объявлен в adapter.out.kafka (FailedEventRecovererConfig)
    @Bean
    DefaultErrorHandler kafkaErrorHandler(ConsumerRecordRecoverer recoverer, KafkaProperties kafkaProperties,
                                          ProcessingProperties processing) {
        String failedEventsTopic = kafkaProperties.failedEventsTopic();
        DefaultErrorHandler handler = new DefaultErrorHandler((record, exception) -> {
            // Не записалось в failed-events - исключение, сообщение будет обработано снова: строка ниже не выполнится
            recoverer.accept(record, exception);
            logMovedToFailedEvents(record, exception, failedEventsTopic);
        }, storageBackOff(processing));
        handler.addNotRetryableExceptions(InvalidEventException.class);
        // Повторять ли ошибку десериализации, решает функция ниже: временный сбой - да, «ядовитое» сообщение - нет
        handler.removeClassification(DeserializationException.class);
        BackOff schemaRegistryBackOff = unlimitedBackOff();
        handler.setBackOffFunction((record, exception) -> {
            if (KafkaErrorClassifier.isPoisonPill(exception)) {
                return NO_RETRIES;
            }
            return KafkaErrorClassifier.isDeserializationFailure(exception) ? schemaRegistryBackOff : null;
        });
        // Offset отложенного сообщения подтверждается только после успешной записи в failed-events
        handler.setCommitRecovered(true);
        return handler;
    }

    /** Номер доставки в заголовке kafka_deliveryAttempt: повтор после ошибки не считается новым сообщением. */
    @Bean
    ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> deliveryAttemptHeader() {
        return container -> container.getContainerProperties().setDeliveryAttemptHeader(true);
    }

    // max-attempts попыток всего: первая и max-attempts - 1 повторов (паузы 1 с, 2 с, ...)
    static BackOff storageBackOff(ProcessingProperties processing) {
        if (processing.maxAttempts() == 1) {
            // ExponentialBackOffWithMaxRetries(0) не останавливается: одна попытка - сразу в failed-events
            return NO_RETRIES;
        }
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(processing.maxAttempts() - 1);
        backOff.setInitialInterval(processing.retryInterval().toMillis());
        backOff.setMultiplier(processing.retryMultiplier());
        backOff.setMaxInterval(Math.max(MAX_BACKOFF_MS, processing.retryInterval().toMillis()));
        return backOff;
    }

    private static BackOff unlimitedBackOff() {
        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxInterval(MAX_BACKOFF_MS);
        return backOff;
    }

    /** Почему сообщение попало в failed-events: deserialization, invalid, storage, error. */
    static String reasonOf(Throwable exception) {
        if (KafkaErrorClassifier.isDeserializationFailure(exception)) {
            return "deserialization";
        }
        if (causedBy(exception, InvalidEventException.class)) {
            return "invalid";
        }
        return causedBy(exception, EventPersistenceException.class) ? "storage" : "error";
    }

    private static void logMovedToFailedEvents(ConsumerRecord<?, ?> record, Exception exception, String failedEventsTopic) {
        Throwable root = rootCause(exception);
        String reason = reasonOf(exception);
        try (var topic = MDC.putCloseable("topic", record.topic());
             var partition = MDC.putCloseable("partition", String.valueOf(record.partition()));
             var offset = MDC.putCloseable("offset", String.valueOf(record.offset()));
             var status = MDC.putCloseable("status", "FAILED_EVENT");
             var failedReason = MDC.putCloseable("reason", reason);
             var errorClass = MDC.putCloseable("errorClass", root.getClass().getName());
             var errorMessage = MDC.putCloseable("errorMessage", String.valueOf(root.getMessage()))) {
            log.error("Message moved to {} ({}): {}", failedEventsTopic, reason, root.toString());
        }
    }

    private static boolean causedBy(Throwable exception, Class<? extends Throwable> type) {
        for (Throwable e = exception; e != null; e = e.getCause() == e ? null : e.getCause()) {
            if (type.isInstance(e)) {
                return true;
            }
        }
        return false;
    }

    private static Throwable rootCause(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root;
    }
}
