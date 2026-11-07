package com.github.gluhov.iotplatform.devicecollector.domain.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Снимок устройства из DeviceInfo.device: поля модели, которые хранятся в devices. */
public record DeviceState(String deviceId,
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
                          List<SensorState> sensors) {

    public static final String DELETED = "DELETED";
    public static final String UNKNOWN = "UNKNOWN";

    public boolean deleted() {
        return DELETED.equals(status);
    }
}
