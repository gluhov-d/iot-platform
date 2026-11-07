package com.github.gluhov.iotplatform.devicecollector.adapter.in.web;

import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceState;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceView;
import com.github.gluhov.iotplatform.devicecollector.domain.model.SensorState;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Устройство из реестра; shard - база шарда, которая вернула строку (current_database()). */
public record DeviceResponse(String deviceId,
                             String tenantId,
                             String ownerId,
                             String deviceType,
                             String serialNumber,
                             String manufacturer,
                             String model,
                             String firmwareVersion,
                             String status,
                             Instant registeredAt,
                             Instant lastSeenAt,
                             Map<String, String> settings,
                             Map<String, String> attributes,
                             List<SensorState> sensors,
                             long seenCount,
                             Instant firstSeenAt,
                             Instant lastEventAt,
                             String lastEventId,
                             String lastEventType,
                             Instant deletedAt,
                             String shard) {

    static DeviceResponse from(DeviceView view) {
        DeviceState device = view.device();
        return new DeviceResponse(device.deviceId(), device.tenantId(), device.ownerId(), device.deviceType(),
                device.serialNumber(), device.manufacturer(), device.model(), device.firmwareVersion(), device.status(),
                device.registeredAt(), device.lastSeenAt(), device.settings(), device.attributes(), device.sensors(),
                view.seenCount(), view.firstSeenAt(), view.lastEventAt(), view.lastEventId(), view.lastEventType(),
                view.deletedAt(), view.shard());
    }
}