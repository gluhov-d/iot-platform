package com.github.gluhov.iotplatform.devicecollector.domain.service;

import com.github.gluhov.iotplatform.devicecollector.domain.exception.InvalidDeviceInfoException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceChange;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ValidationPolicy;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceSnapshot;
import com.github.gluhov.iotplatform.events.avro.SensorSnapshot;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.HashSet;
import java.util.Set;

/**
 * Проверка обязательных полей DeviceInfo. Ошибка - InvalidDeviceInfoException: сообщение сразу уходит
 * в devices-dlt, повтор не поможет.
 */
@Component
@RequiredArgsConstructor
public class DeviceInfoValidator {
    private final Clock clock;
    private final ValidationPolicy policy;

    public void validate(String key, DeviceInfo info) {
        if (info == null) {
            throw new InvalidDeviceInfoException("Message value is empty (tombstone or null)");
        }
        String deviceId = info.getDeviceId();
        requireText(deviceId, "deviceId");
        if (deviceId.length() > DeviceChange.MAX_DEVICE_ID_LENGTH) {
            throw new InvalidDeviceInfoException("deviceId is longer than " + DeviceChange.MAX_DEVICE_ID_LENGTH + " characters");
        }
        // Ключ сообщения - deviceId: по нему Kafka выбирает раздел, и все события устройства идут по порядку
        if (!deviceId.equals(key)) {
            throw new InvalidDeviceInfoException("Message key '" + key + "' differs from deviceId '" + deviceId + "'");
        }
        if (info.getEventId() != null) {
            requireText(info.getEventId(), "eventId");
        }
        requireTime(info.getEmittedAtMs(), "emittedAtMs");
        if (info.getEventTimeMs() != null) {
            requireTime(info.getEventTimeMs(), "eventTimeMs");
        }
        if (info.getDevice() != null) {
            validateSnapshot(deviceId, info.getDevice());
        }
    }

    // Снимок необязателен (сообщения 1.0.0 и события без снимка), но если он есть - он про это устройство и полный
    private static void validateSnapshot(String deviceId, DeviceSnapshot device) {
        if (!deviceId.equals(device.getDeviceId())) {
            throw new InvalidDeviceInfoException(
                    "device.deviceId '" + device.getDeviceId() + "' differs from deviceId '" + deviceId + "'");
        }
        requireText(device.getTenantId(), "device.tenantId");
        requireText(device.getDeviceType(), "device.deviceType");
        Set<String> sensorIds = new HashSet<>();
        for (SensorSnapshot sensor : device.getSensors()) {
            requireText(sensor.getSensorId(), "device.sensors[].sensorId");
            requireText(sensor.getSensorType(), "device.sensors[].sensorType");
            if (!sensorIds.add(sensor.getSensorId())) {
                throw new InvalidDeviceInfoException("device.sensors has duplicate sensorId '" + sensor.getSensorId() + "'");
            }
        }
    }

    private void requireTime(long epochMs, String field) {
        if (epochMs <= 0) {
            throw new InvalidDeviceInfoException(field + " must be positive: " + epochMs);
        }
        long latestAllowed = clock.millis() + policy.maxFutureSkew().toMillis();
        if (epochMs > latestAllowed) {
            throw new InvalidDeviceInfoException(field + " is more than " + policy.maxFutureSkew() + " in the future: " + epochMs);
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new InvalidDeviceInfoException(field + " is blank");
        }
    }
}