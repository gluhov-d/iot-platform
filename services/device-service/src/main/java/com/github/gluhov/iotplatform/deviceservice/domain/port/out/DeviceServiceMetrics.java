package com.github.gluhov.iotplatform.deviceservice.domain.port.out;

import com.github.gluhov.iotplatform.deviceservice.domain.model.CacheResult;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceCommand;

import java.time.Duration;

/** Метрики сервиса. Метки - только исход и операция; deviceId в метках не используется. */
public interface DeviceServiceMetrics {

    void cacheResult(CacheResult result);

    /** Команда подтверждена Kafka. */
    void commandAccepted(DeviceCommand.Operation operation);

    /** Команда отклонена: code - значение Problem.code контракта (DEVICE_NOT_FOUND, DEVICE_ALREADY_EXISTS, COMMAND_NOT_ACCEPTED). */
    void commandRejected(DeviceCommand.Operation operation, String code);

    /** Время отправки команды до подтверждения (или до ошибки). */
    void commandPublished(DeviceCommand.Operation operation, Duration duration);
}
