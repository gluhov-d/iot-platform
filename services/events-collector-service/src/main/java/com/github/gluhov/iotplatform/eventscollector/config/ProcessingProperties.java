package com.github.gluhov.iotplatform.eventscollector.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Повторы при ошибке записи входного события (ClickHouse): всего maxAttempts попыток, пауза между ними
 * растёт в retryMultiplier раз от retryInterval; после последней попытки сообщение уходит в events-dlt.
 *
 * @param maxAttempts     число попыток обработки одного сообщения, включая первую (не меньше 1)
 * @param retryInterval   пауза перед второй попыткой
 * @param retryMultiplier во сколько раз растёт пауза перед каждой следующей попыткой (не меньше 1)
 */
@ConfigurationProperties(prefix = "app.processing")
public record ProcessingProperties(@DefaultValue("3") int maxAttempts,
                                   @DefaultValue("1s") Duration retryInterval,
                                   @DefaultValue("2.0") double retryMultiplier) {

    public ProcessingProperties {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("app.processing.max-attempts must be at least 1");
        }
        if (retryInterval.isNegative() || retryInterval.isZero()) {
            throw new IllegalArgumentException("app.processing.retry-interval must be positive");
        }
        if (retryMultiplier < 1.0) {
            throw new IllegalArgumentException("app.processing.retry-multiplier must be at least 1");
        }
    }
}