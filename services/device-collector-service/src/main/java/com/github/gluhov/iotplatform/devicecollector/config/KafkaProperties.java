package com.github.gluhov.iotplatform.devicecollector.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * @param devicesTopic               входящая тема с DeviceInfo
 * @param failedEventsTopic          тема сообщений, которые приём не смог обработать (ADR-0013)
 * @param failedEventsPublishTimeout сколько ждать подтверждения записи в failed-events, прежде чем считать публикацию неудачной
 */
@ConfigurationProperties(prefix = "app.kafka")
public record KafkaProperties(@DefaultValue("devices") String devicesTopic,
                              String failedEventsTopic,
                              Duration failedEventsPublishTimeout) {}