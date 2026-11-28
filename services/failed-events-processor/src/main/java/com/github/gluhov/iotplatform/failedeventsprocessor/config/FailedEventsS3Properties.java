package com.github.gluhov.iotplatform.failedeventsprocessor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Настройки архива из переменных окружения (см. failed-events-processor.yaml). Значений по умолчанию нет.
 * Тайм-ауты клиента должны быть короче max.poll.interval.ms (R6).
 */
@ConfigurationProperties("failed-events")
public record FailedEventsS3Properties(
        String bucket,
        S3 s3,
        Retry retry) {

    public record S3(
            String endpoint,
            String region,
            String accessKey,
            String secretKey,
            Duration connectTimeout,
            Duration apiCallTimeout,
            Duration apiCallAttemptTimeout) {
    }

    public record Retry(Duration initialInterval, Duration maxInterval, double multiplier) {
    }
}
