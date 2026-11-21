package com.github.gluhov.iotplatform.eventscollector.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param failedEventsTopic тема, куда уходят сообщения, которые сборщик не смог обработать (failed-events)
 */
@ConfigurationProperties(prefix = "app.kafka")
public record KafkaProperties(String eventsTopic,
                              String devicesTopic,
                              String failedEventsTopic,
                              @DefaultValue("20s") Duration sendTimeout) {}
