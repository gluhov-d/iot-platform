package com.github.gluhov.iotplatform.deviceservice.adapter.in.web.security;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Настройки проверки сервисных токенов Keycloak (FR-009). Все значения - из переменных окружения (application.yaml):
 * адрес издателя (iss) и адрес ключей (внутренний адрес сети Compose), аудитория, допуск часов, тайм-ауты запроса ключей.
 */
@ConfigurationProperties("app.security")
public record DeviceSecurityProperties(
        String issuerUri,
        String jwkSetUri,
        String audience,
        Duration clockSkew,
        Duration jwkConnectTimeout,
        Duration jwkReadTimeout
) {
}
