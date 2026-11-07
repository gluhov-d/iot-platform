package com.github.gluhov.iotplatform.devicecollector.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param devicesTopic входящая тема с DeviceInfo
 * @param dltTopic     тема необработанных сообщений
 */
@ConfigurationProperties(prefix = "app.kafka")
public record KafkaProperties(@DefaultValue("devices") String devicesTopic,
                              @DefaultValue("devices-dlt") String dltTopic) {}