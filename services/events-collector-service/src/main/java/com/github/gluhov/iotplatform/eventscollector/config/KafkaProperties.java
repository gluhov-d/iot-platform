package com.github.gluhov.iotplatform.eventscollector.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

@ConfigurationProperties(prefix = "app.kafka")
public record KafkaProperties(String eventsTopic,
                              String devicesTopic,
                              @DefaultValue("20s") Duration sendTimeout) {}
