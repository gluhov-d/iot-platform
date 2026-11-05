package com.github.gluhov.iotplatform.eventscollector.domain.service;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.events.avro.DeviceSnapshot;
import com.github.gluhov.iotplatform.events.avro.SensorSnapshot;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.InvalidEventException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

@Component
@RequiredArgsConstructor
public class DeviceEventValidator {

    public static final Duration MAX_FUTURE_SKEW = Duration.ofDays(1);
    private final Clock clock;

    public void validate(DeviceEvent event) {
        if (event == null) {
            throw new InvalidEventException("Event is null");
        }
        requireText(event.getEventId(), "eventId");
        requireText(event.getDeviceId(), "deviceId");
        requireText(event.getEventType(), "eventType");

        long timestampMs = event.getTimestampMs();
        if (timestampMs <= 0) {
            throw new InvalidEventException("timestampMs must be positive: " + timestampMs);
        }
        long latestAllowed = clock.millis() + MAX_FUTURE_SKEW.toMillis();
        if (timestampMs > latestAllowed) {
            throw new InvalidEventException("timestampMs is more than " + MAX_FUTURE_SKEW + " in the future: " + timestampMs);
        }
        if (event.getDevice() != null) {
            validateSnapshot(event.getDeviceId(), event.getDevice());
        }
    }

    // Снимок необязателен (телеметрия идёт без него), но если он есть - он про это же устройство и полный:
    // по нему device-collector-service создаёт запись в реестре
    private static void validateSnapshot(String deviceId, DeviceSnapshot device) {
        if (!deviceId.equals(device.getDeviceId())) {
            throw new InvalidEventException("device.deviceId '" + device.getDeviceId() + "' differs from deviceId '" + deviceId + "'");
        }
        requireText(device.getTenantId(), "device.tenantId");
        requireText(device.getDeviceType(), "device.deviceType");
        Set<String> sensorIds = new HashSet<>();
        for (SensorSnapshot sensor : device.getSensors()) {
            requireText(sensor.getSensorId(), "device.sensors[].sensorId");
            requireText(sensor.getSensorType(), "device.sensors[].sensorType");
            if (!sensorIds.add(sensor.getSensorId())) {
                throw new InvalidEventException("device.sensors has duplicate sensorId " + sensor.getSensorId());
            }
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new InvalidEventException(field + " is blank");
        }
    }
}
