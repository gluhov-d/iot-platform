package com.github.gluhov.iotplatform.eventscollector.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "app.outbox")
public record OutboxProperties(int batchSize,
                               int maxAttempts,
                               long leaseTtlMs,
                               long publishIntervalMs,
                               @DefaultValue("true") boolean schedulingEnabled) {}
