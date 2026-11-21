package com.github.gluhov.iotplatform.eventscollector.it;

import com.github.gluhov.iotplatform.failedeventsprocessor.FailedEventsProcessorApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Процессор failed-events вторым Spring-контекстом в JVM сборщика (R23, T012).
 * Конфигурация - failed-events-processor.yaml: имя файла задаётся свойством spring.config.name контекста,
 * глобальное System-свойство не трогается (оно повлияло бы на контекст сборщика, поднятый позже).
 */
final class FailedEventsProcessorContext {

    private FailedEventsProcessorContext() {
    }

    static ConfigurableApplicationContext start(String kafkaBootstrap, String schemaRegistryUrl, String topic,
                                                String group, String s3Endpoint, String bucket,
                                                String accessKey, String secretKey) {
        return new SpringApplicationBuilder(FailedEventsProcessorApplication.class)
                .web(WebApplicationType.NONE)
                .properties(properties(
                        "spring.config.name", "failed-events-processor",
                        "KAFKA_BOOTSTRAP_SERVERS", kafkaBootstrap,
                        "SCHEMA_REGISTRY_URL", schemaRegistryUrl,
                        "KAFKA_FAILED_EVENTS_TOPIC", topic,
                        "FAILED_EVENTS_CONSUMER_GROUP", group,
                        "FAILED_EVENTS_MAX_POLL_INTERVAL_MS", "30000",
                        "FAILED_EVENTS_RETRY_INITIAL_INTERVAL", "500ms",
                        "FAILED_EVENTS_RETRY_MAX_INTERVAL", "10s",
                        "FAILED_EVENTS_RETRY_MULTIPLIER", "2",
                        "FAILED_EVENTS_S3_ENDPOINT", s3Endpoint,
                        "FAILED_EVENTS_S3_REGION", "us-east-1",
                        "FAILED_EVENTS_S3_ACCESS_KEY", accessKey,
                        "FAILED_EVENTS_S3_SECRET_KEY", secretKey,
                        "FAILED_EVENTS_S3_CONNECT_TIMEOUT", "2s",
                        "FAILED_EVENTS_S3_API_CALL_TIMEOUT", "8s",
                        "FAILED_EVENTS_S3_API_CALL_ATTEMPT_TIMEOUT", "4s",
                        "FAILED_EVENTS_BUCKET", bucket,
                        "MANAGEMENT_OTLP_ENDPOINT", "http://localhost:4318/v1/traces",
                        // Автоконфигурации JDBC и Redis есть только на classpath сборщика (в продуктивном
                        // процессоре их нет): без исключения контекст процессора пытается поднять DataSource сборщика
                        "spring.autoconfigure.exclude", String.join(",",
                                "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
                                "org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration",
                                "org.springframework.boot.jdbc.autoconfigure.JdbcClientAutoConfiguration",
                                "org.springframework.boot.jdbc.autoconfigure.JdbcTemplateAutoConfiguration",
                                "org.springframework.boot.jdbc.autoconfigure.health.DataSourceHealthContributorAutoConfiguration",
                                "org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration",
                                "org.springframework.boot.data.redis.autoconfigure.DataRedisRepositoriesAutoConfiguration",
                                "org.springframework.boot.data.redis.autoconfigure.health.DataRedisHealthContributorAutoConfiguration")))
                .run();
    }

    /** Пары «имя, значение» в свойства (Map.of не принимает больше десяти пар). */
    private static Map<String, Object> properties(String... nameValuePairs) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < nameValuePairs.length; i += 2) {
            result.put(nameValuePairs[i], nameValuePairs[i + 1]);
        }
        return result;
    }
}
