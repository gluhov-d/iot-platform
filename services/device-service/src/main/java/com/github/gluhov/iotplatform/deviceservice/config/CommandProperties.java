package com.github.gluhov.iotplatform.deviceservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Команды в тему devices: имя темы и срок ожидания подтверждения Kafka (DEVICE_COMMAND_SEND_TIMEOUT).
 * По истечении срока клиент получает 503 COMMAND_NOT_ACCEPTED.
 */
@ConfigurationProperties("app.commands")
public record CommandProperties(String devicesTopic, Duration sendTimeout) {
}
