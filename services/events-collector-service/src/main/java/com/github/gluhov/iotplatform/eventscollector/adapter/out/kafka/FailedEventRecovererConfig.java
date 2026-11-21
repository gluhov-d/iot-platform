package com.github.gluhov.iotplatform.eventscollector.adapter.out.kafka;

import com.github.gluhov.iotplatform.eventscollector.config.KafkaProperties;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.CollectorMetrics;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.time.Duration;
import java.util.Map;

/**
 * Продюсер failed-events отдельно от продюсера outbox: свои тайм-ауты ({@code app.kafka.failed-events-*}),
 * та же Avro-сериализация. Бин объявлен здесь, а не в config: обработчик ошибок (config) получает его как
 * ConsumerRecordRecoverer и не зависит от адаптера.
 */
@Configuration(proxyBeanMethods = false)
public class FailedEventRecovererConfig {

    @Bean
    FailedEventRecoverer failedEventRecoverer(ProducerFactory<?, ?> producerFactory, Tracer tracer,
                                              CollectorMetrics metrics, KafkaProperties kafkaProperties,
                                              @Value("${spring.application.name}") String sourceService,
                                              @Value("${app.kafka.failed-events-publish-timeout}") Duration publishTimeout,
                                              @Value("${app.kafka.failed-events-max-block-ms}") long maxBlockMs,
                                              @Value("${app.kafka.failed-events-request-timeout-ms}") int requestTimeoutMs,
                                              @Value("${app.kafka.failed-events-delivery-timeout-ms}") int deliveryTimeoutMs) {
        @SuppressWarnings("unchecked")
        ProducerFactory<String, Object> factory = (ProducerFactory<String, Object>) producerFactory;
        ProducerFactory<String, Object> errorFactory = factory.copyWithConfigurationOverride(Map.of(
                ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs));
        return new FailedEventRecoverer(new KafkaTemplate<>(errorFactory), tracer, metrics,
                kafkaProperties.failedEventsTopic(), sourceService, publishTimeout);
    }
}
