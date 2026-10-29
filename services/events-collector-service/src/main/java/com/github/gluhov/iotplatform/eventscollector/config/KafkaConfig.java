package com.github.gluhov.iotplatform.eventscollector.config;

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
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.LinkedHashMap;
import java.util.Map;

@Configuration(proxyBeanMethods = false)
@Slf4j
public class KafkaConfig {
    private static final long INITIAL_BACKOFF_MS = 500L;
    private static final double BACKOFF_MULTIPLIER = 2.0;
    private static final long MAX_BACKOFF_MS = 10_000L;
    private static final BackOff NO_RETRIES = new FixedBackOff(0L, 0L);
    // Тема для отложенных сообщений: events -> events.DLT (ее создает init-topics.sh)
    public static final String DLT_SUFFIX = ".DLT";

    // Ошибки записи и временные сбои Schema Registry повторяются без ограничения попыток: входное событие нельзя терять.
    // Невалидные и недесериализуемые сообщения не повторяются: они откладываются в <тема>.DLT, и только после этого
    // их offset подтверждается. Не удалось записать в DLT - сообщение не подтверждено и будет обработано снова.
    @Bean
    DefaultErrorHandler kafkaErrorHandler(ProducerFactory<?, ?> producerFactory, KafkaTemplate<?, ?> kafkaTemplate) {
        ExponentialBackOff backOff = new ExponentialBackOff(INITIAL_BACKOFF_MS, BACKOFF_MULTIPLIER);
        backOff.setMaxInterval(MAX_BACKOFF_MS);

        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        // Сообщение не десериализовалось: в DLT уходят исходные байты
        templates.put(byte[].class, new KafkaTemplate<>(producerFactory.copyWithConfigurationOverride(
                Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))));
        // Событие не прошло валидацию: в DLT уходит DeviceEvent в Avro
        templates.put(Object.class, kafkaTemplate);
        // Раздел DLT - с тем же номером, что у исходного сообщения
        DeadLetterPublishingRecoverer deadLetters = new DeadLetterPublishingRecoverer(templates,
                (record, exception) -> new TopicPartition(record.topic() + DLT_SUFFIX, record.partition()));

        DefaultErrorHandler handler = new DefaultErrorHandler((record, exception) -> {
            deadLetters.accept(record, exception);
            logRejected(record, exception);
        }, backOff);
        handler.addNotRetryableExceptions(InvalidEventException.class);
        // Повторять ли ошибку десериализации, решает функция ниже: временный сбой - да, "ядовитое" сообщение - нет
        handler.removeClassification(DeserializationException.class);
        handler.setBackOffFunction((record, exception) -> KafkaErrorClassifier.isPoisonPill(exception) ? NO_RETRIES : null);
        // Offset отложенного сообщения подтверждается только после успешной записи в DLT
        handler.setCommitRecovered(true);
        return handler;
    }

    private static void logRejected(ConsumerRecord<?, ?> record, Exception exception) {
        Throwable root = rootCause(exception);
        try (var topic = MDC.putCloseable("topic", record.topic());
             var partition = MDC.putCloseable("partition", String.valueOf(record.partition()));
             var offset = MDC.putCloseable("offset", String.valueOf(record.offset()));
             var status = MDC.putCloseable("status", "REJECTED");
             var errorClass = MDC.putCloseable("errorClass", root.getClass().getName());
             var errorMessage = MDC.putCloseable("errorMessage", String.valueOf(root.getMessage()))) {
            log.error("Message rejected and moved to {}{}: {}", record.topic(), DLT_SUFFIX, root.toString());
        }
    }

    private static Throwable rootCause(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root;
    }
}