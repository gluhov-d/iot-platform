package com.github.gluhov.iotplatform.deviceservice.domain.model;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Команда владельцу данных (device-collector-service): то, что уйдёт в тему devices.
 * commandId совпадает с eventId сообщения DeviceInfo и с commandId ответа API; acceptedAt - время приёма запроса.
 * Для DELETE поля администратора не заполняются (fields = null).
 */
public record DeviceCommand(UUID commandId,
                            Operation operation,
                            String deviceId,
                            Fields fields,
                            Instant acceptedAt) {

    /** Операция над устройством; совпадает с значениями OperationType в DeviceInfo без UNKNOWN. */
    public enum Operation {
        CREATE, UPDATE, DELETE
    }

    /** Поля, которыми управляет администратор (PUT заменяет их целиком; CREATE задаёт при создании). */
    public record Fields(String name,
                         String tenantId,
                         String ownerId,
                         String deviceType,
                         String serialNumber,
                         String manufacturer,
                         String model,
                         String status,
                         Map<String, String> settings,
                         Map<String, String> attributes) {
    }
}
