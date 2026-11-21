package com.github.gluhov.iotplatform.deviceservice.domain.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Устройство из модели чтения: поля администратора, данные устройства и версия записи владельца данных.
 * Набор полей совпадает с DeviceResponse контракта; статус уже приведён к значениям контракта (прочее - UNKNOWN).
 */
public record Device(String deviceId,
                     String name,
                     String tenantId,
                     String ownerId,
                     String deviceType,
                     String serialNumber,
                     String manufacturer,
                     String model,
                     String status,
                     Map<String, String> settings,
                     Map<String, String> attributes,
                     String firmwareVersion,
                     String osName,
                     String osVersion,
                     List<DeviceSensor> sensors,
                     Instant registeredAt,
                     Instant lastSeenAt,
                     long version) {
}
