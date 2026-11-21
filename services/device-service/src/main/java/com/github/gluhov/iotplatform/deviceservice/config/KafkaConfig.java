package com.github.gluhov.iotplatform.deviceservice.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka: настройки продюсера (acks=all, идемпотентность, auto.register.schemas=false, observation) - в application.yaml
 * (spring.kafka.*); публикатор команд - адаптер KafkaDeviceCommandPublisher; здесь - срок ожидания AdminClient.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(CommandProperties.class)
public class KafkaConfig {

    /**
     * Наблюдение KafkaTemplate (трасса отправки) до отправки запрашивает идентификатор кластера через AdminClient,
     * и этот запрос без срока ждёт default.api.timeout.ms (60 с по умолчанию). При недоступной Kafka ответ должен
     * укладываться в срок подтверждения команды: тот же срок задаём и для AdminClient.
     */
    @Bean
    KafkaAdmin kafkaAdmin(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                          CommandProperties properties) {
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Math.toIntExact(properties.sendTimeout().toMillis()));
        return new KafkaAdmin(config);
    }
}
