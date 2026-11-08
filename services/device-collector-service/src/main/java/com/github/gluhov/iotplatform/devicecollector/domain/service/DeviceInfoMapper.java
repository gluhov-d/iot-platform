package com.github.gluhov.iotplatform.devicecollector.domain.service;

import com.github.gluhov.iotplatform.devicecollector.domain.model.ChangeType;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceChange;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceState;
import com.github.gluhov.iotplatform.devicecollector.domain.model.MessagePosition;
import com.github.gluhov.iotplatform.devicecollector.domain.model.SensorState;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceSnapshot;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** DeviceInfo (Avro) -> DeviceChange: миллисекунды Unix -> Instant, ключ идемпотентности, время события. */
@Component
public class DeviceInfoMapper {
    /** Ключ идемпотентности сообщений 1.0.0, у которых нет eventId. */
    public static final String LEGACY_KEY_PREFIX = "emitted:";

    public DeviceChange toChange(DeviceInfo info, MessagePosition position) {
        Instant emittedAt = toInstant(info.getEmittedAtMs());
        // Время события, а не публикации: повторная публикация того же события приходит с новым emittedAtMs
        Instant eventAt = info.getEventTimeMs() == null ? emittedAt : toInstant(info.getEventTimeMs());
        String eventKey = info.getEventId() == null ? LEGACY_KEY_PREFIX + info.getEmittedAtMs() : info.getEventId();
        return new DeviceChange(info.getDeviceId(), eventKey, info.getEventId(),
                ChangeType.valueOf(info.getEventType().name()), eventAt, emittedAt, toState(info.getDevice()), position);
    }

    public static Instant toInstant(long epochMs) {
        return Instant.ofEpochMilli(epochMs);
    }

    static DeviceState toState(DeviceSnapshot device) {
        if (device == null) {
            return null;
        }
        List<SensorState> sensors = device.getSensors().stream()
                .map(sensor -> new SensorState(sensor.getSensorId(), sensor.getSensorType(), sensor.getUnit(),
                        sensor.getValue(), toInstantOrNull(sensor.getMeasuredAtMs()), sensor.getStatus().name()))
                .toList();
        return new DeviceState(device.getDeviceId(), device.getTenantId(), device.getOwnerId(), device.getDeviceType(),
                device.getSerialNumber(), device.getManufacturer(), device.getModel(), device.getFirmwareVersion(),
                device.getName(), device.getOsName(), device.getOsVersion(), device.getStatus().name(),
                toInstantOrNull(device.getRegisteredAtMs()), toInstantOrNull(device.getLastSeenAtMs()),
                copy(device.getSettings()), copy(device.getAttributes()), sensors);
    }

    private static Instant toInstantOrNull(Long epochMs) {
        return epochMs == null ? null : toInstant(epochMs);
    }

    private static Map<String, String> copy(Map<String, String> source) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }
}