package com.github.gluhov.iotplatform.devicecollector.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Обработка сообщения.
 *
 * @param maxAttempts     попыток записи при ошибке хранилища, включая первую; затем сообщение уходит в DLT
 * @param retryInterval   пауза перед второй попыткой
 * @param retryMultiplier во сколько раз растёт пауза перед каждой следующей
 * @param maxFutureSkew   насколько время события может опережать часы сервиса
 * @param shardCacheTtl   сколько помнить шард устройства, определённый через PREVIEW на пути ошибки
 */
@ConfigurationProperties(prefix = "app.processing")
public record ProcessingProperties(@DefaultValue("3") int maxAttempts,
                                   @DefaultValue("1s") Duration retryInterval,
                                   @DefaultValue("2.0") double retryMultiplier,
                                   @DefaultValue("1d") Duration maxFutureSkew,
                                   @DefaultValue("5m") Duration shardCacheTtl) {

    public ProcessingProperties {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("app.processing.max-attempts must be at least 1");
        }
    }
}