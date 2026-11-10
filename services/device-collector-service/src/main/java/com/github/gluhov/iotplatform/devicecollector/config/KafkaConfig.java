package com.github.gluhov.iotplatform.devicecollector.config;

import com.github.gluhov.iotplatform.devicecollector.domain.exception.DeviceNotStoredException;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.InvalidDeviceInfoException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.FailureReason;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessingResult;
import com.github.gluhov.iotplatform.devicecollector.domain.port.out.DeviceMetrics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
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
 * Обработка ошибок приёма из devices:
 * <ul>
 *   <li>невалидное или недесериализуемое сообщение - сразу в devices-dlt;</li>
 *   <li>ошибка записи в PostgreSQL (шард или Proxy недоступны) и непредвиденная ошибка - app.processing.max-attempts
 *       попыток с растущей паузой, затем devices-dlt: сообщения остальных устройств раздела продолжают обрабатываться;</li>
 *   <li>Schema Registry недоступен - повторять без ограничения: это сбой не сообщения, а инфраструктуры,
 *       иначе в devices-dlt ушли бы все сообщения подряд.</li>
 * </ul>
 * Offset отправленного в devices-dlt сообщения подтверждается только после успешной записи в devices-dlt;
 * не удалось записать - сообщение не подтверждено и будет обработано снова.
 */
@Configuration(proxyBeanMethods = false)
@Slf4j
public class KafkaConfig {
    private static final long MAX_BACKOFF_MS = 10_000L;
    private static final BackOff NO_RETRIES = new FixedBackOff(0L, 0L);

    @Bean
    DefaultErrorHandler kafkaErrorHandler(ProducerFactory<?, ?> producerFactory, KafkaTemplate<?, ?> kafkaTemplate,
                                          KafkaProperties kafkaProperties, ProcessingProperties processing,
                                          DeviceMetrics metrics) {
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        // Сообщение не десериализовалось: в devices-dlt уходят исходные байты
        templates.put(byte[].class, new KafkaTemplate<>(producerFactory.copyWithConfigurationOverride(
                Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))));
        // Остальные: DeviceInfo в Avro (схема devices-dlt-value в Schema Registry)
        templates.put(Object.class, kafkaTemplate);
        String dltTopic = kafkaProperties.dltTopic();
        // Раздел devices-dlt - с тем же номером, что у исходного сообщения (число разделов у тем одинаковое)
        DeadLetterPublishingRecoverer deadLetters = new DeadLetterPublishingRecoverer(templates,
                (record, exception) -> new TopicPartition(dltTopic, record.partition()));

        DefaultErrorHandler handler = new DefaultErrorHandler((record, exception) -> {
            FailureReason reason = reasonOf(exception);
            // Не записалось в devices-dlt - исключение, сообщение будет обработано снова: метрики ниже не увеличены
            deadLetters.accept(record, exception);
            if (reason == FailureReason.DESERIALIZATION) {
                // До обработчика сообщение не дошло - получение и ошибочную попытку учитываем здесь
                metrics.received();
                metrics.processingError(reason);
            }
            metrics.failed(reason);
            metrics.dltPublished(reason);
            logMovedToDlt(record, exception, reason, dltTopic);
        }, storageBackOff(processing));
        handler.addNotRetryableExceptions(InvalidDeviceInfoException.class);
        // Повторять ли ошибку десериализации, решает функция ниже: временный сбой - да, "ядовитое" сообщение - нет
        handler.removeClassification(DeserializationException.class);
        BackOff schemaRegistryBackOff = unlimitedBackOff();
        handler.setBackOffFunction((record, exception) -> {
            if (KafkaErrorClassifier.isPoisonPill(exception)) {
                return NO_RETRIES;
            }
            return KafkaErrorClassifier.isDeserializationFailure(exception) ? schemaRegistryBackOff : null;
        });
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
            // ExponentialBackOffWithMaxRetries(0) не останавливается: одна попытка - сразу в devices-dlt
            return NO_RETRIES;
        }
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(processing.maxAttempts() - 1);
        backOff.setInitialInterval(processing.retryInterval().toMillis());
        backOff.setMultiplier(processing.retryMultiplier());
        backOff.setMaxInterval(MAX_BACKOFF_MS);
        return backOff;
    }

    private static BackOff unlimitedBackOff() {
        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxInterval(MAX_BACKOFF_MS);
        return backOff;
    }

    static FailureReason reasonOf(Exception exception) {
        return KafkaErrorClassifier.isDeserializationFailure(exception) ? FailureReason.DESERIALIZATION
                : FailureReason.of(exception);
    }

    private static void logMovedToDlt(ConsumerRecord<?, ?> record, Exception exception, FailureReason reason, String dltTopic) {
        Throwable root = rootCause(exception);
        try (var topic = MDC.putCloseable("topic", record.topic());
             var partition = MDC.putCloseable("partition", String.valueOf(record.partition()));
             var offset = MDC.putCloseable("offset", String.valueOf(record.offset()));
             var deviceId = MDC.putCloseable("deviceId", record.key() == null ? null : String.valueOf(record.key()));
             var shard = MDC.putCloseable("shard", shardOf(exception));
             var status = MDC.putCloseable("status", "DLT");
             var errorClass = MDC.putCloseable("errorClass", root.getClass().getName());
             var errorMessage = MDC.putCloseable("errorMessage", String.valueOf(root.getMessage()))) {
            log.error("Message moved to {} ({}): {}", dltTopic, reason.label(), root.toString());
        }
    }

    private static String shardOf(Throwable exception) {
        for (Throwable e = exception; e != null; e = e.getCause() == e ? null : e.getCause()) {
            if (e instanceof DeviceNotStoredException notStored) {
                return notStored.shard();
            }
        }
        return ProcessingResult.UNKNOWN_SHARD;
    }

    private static Throwable rootCause(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root;
    }
}