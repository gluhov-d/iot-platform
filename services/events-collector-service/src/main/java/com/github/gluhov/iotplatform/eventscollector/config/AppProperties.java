package com.github.gluhov.iotplatform.eventscollector.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

@ConfigurationProperties("app")
public record AppProperties(
        String instanceId,
        Kafka kafka,
        Outbox outbox
) {
    public record Kafka(
            String eventsTopic,
            String devicesTopic,
            @DefaultValue("20s")Duration sendTimeout) {}

    public record Outbox(
            int batchSize,
            int maxAttempts,
            long leaseTtlMs,
            long publishIntervalMs,
            @DefaultValue("true") boolean schedulingEnabled) {}
}