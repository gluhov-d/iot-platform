package com.github.gluhov.iotplatform.eventscollector.config;

import com.github.gluhov.iotplatform.eventscollector.domain.exception.EventPersistenceException;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.InvalidEventException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Обработка ошибок приёма из events:
 * <ul>
 *   <li>невалидное или недесериализуемое сообщение - сразу в events-dlt;</li>
 *   <li>ошибка записи в ClickHouse и непредвиденная ошибка - app.processing.max-attempts попыток с растущей
 *       паузой, затем events-dlt: следующие сообщения раздела продолжают обрабатываться;</li>
 *   <li>Schema Registry недоступен - повторять без ограничения: это сбой не сообщения, а инфраструктуры,
 *       иначе в events-dlt ушли бы все сообщения подряд.</li>
 * </ul>
 * Offset отправленного в events-dlt сообщения подтверждается только после успешной записи в events-dlt;
 * не удалось записать - сообщение не подтверждено и будет обработано снова.
 */
@Configuration(proxyBeanMethods = false)
@Slf4j
public class KafkaConfig {
    private static final long MAX_BACKOFF_MS = 10_000L;
    private static final BackOff NO_RETRIES = new FixedBackOff(0L, 0L);

    @Bean
    DefaultErrorHandler kafkaErrorHandler(ProducerFactory<?, ?> producerFactory, KafkaTemplate<?, ?> kafkaTemplate,
                                          KafkaProperties kafkaProperties, ProcessingProperties processing) {
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        // Сообщение не десериализовалось: в events-dlt уходят исходные байты
        templates.put(byte[].class, new KafkaTemplate<>(producerFactory.copyWithConfigurationOverride(
                Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))));
        // Событие не прошло валидацию или не записалось: в events-dlt уходит DeviceEvent в Avro
        templates.put(Object.class, kafkaTemplate);
        String dltTopic = kafkaProperties.dltTopic();
        // Раздел events-dlt - с тем же номером, что у исходного сообщения (число разделов у тем одинаковое)
        DeadLetterPublishingRecoverer deadLetters = new DeadLetterPublishingRecoverer(templates,
                (record, exception) -> new TopicPartition(dltTopic, record.partition()));

        DefaultErrorHandler handler = new DefaultErrorHandler((record, exception) -> {
            // Не записалось в events-dlt - исключение, сообщение будет обработано снова: строка ниже не выполнится
            deadLetters.accept(record, exception);
            logMovedToDlt(record, exception, dltTopic);
        }, storageBackOff(processing));
        handler.addNotRetryableExceptions(InvalidEventException.class);
        // Повторять ли ошибку десериализации, решает функция ниже: временный сбой - да, "ядовитое" сообщение - нет
        handler.removeClassification(DeserializationException.class);
        BackOff schemaRegistryBackOff = unlimitedBackOff();
        handler.setBackOffFunction((record, exception) -> {
            if (KafkaErrorClassifier.isPoisonPill(exception)) {
                return NO_RETRIES;
            }
            return KafkaErrorClassifier.isDeserializationFailure(exception) ? schemaRegistryBackOff : null;
        });
        // Offset отложенного сообщения подтверждается только после успешной записи в events-dlt
        handler.setCommitRecovered(true);
        return handler;
    }

    // max-attempts попыток всего: первая и max-attempts - 1 повторов (паузы 1 с, 2 с, ...)
    static BackOff storageBackOff(ProcessingProperties processing) {
        if (processing.maxAttempts() == 1) {
            // ExponentialBackOffWithMaxRetries(0) не останавливается: одна попытка - сразу в events-dlt
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

    /** Почему сообщение попало в events-dlt: deserialization, invalid, storage, error. */
    static String reasonOf(Throwable exception) {
        if (KafkaErrorClassifier.isDeserializationFailure(exception)) {
            return "deserialization";
        }
        if (causedBy(exception, InvalidEventException.class)) {
            return "invalid";
        }
        return causedBy(exception, EventPersistenceException.class) ? "storage" : "error";
    }

    private static void logMovedToDlt(ConsumerRecord<?, ?> record, Exception exception, String dltTopic) {
        Throwable root = rootCause(exception);
        String reason = reasonOf(exception);
        try (var topic = MDC.putCloseable("topic", record.topic());
             var partition = MDC.putCloseable("partition", String.valueOf(record.partition()));
             var offset = MDC.putCloseable("offset", String.valueOf(record.offset()));
             var status = MDC.putCloseable("status", "REJECTED");
             var dltReason = MDC.putCloseable("reason", reason);
             var errorClass = MDC.putCloseable("errorClass", root.getClass().getName());
             var errorMessage = MDC.putCloseable("errorMessage", String.valueOf(root.getMessage()))) {
            log.error("Message moved to {} ({}): {}", dltTopic, reason, root.toString());
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
