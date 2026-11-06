package com.github.gluhov.iotplatform.eventscollector.domain.port.out;

import com.github.gluhov.iotplatform.events.avro.DeviceInfo;

public interface DeviceInfoPublisher {

    /** Отправляет DeviceInfo в тему devices с ключом deviceId и ждёт подтверждения брокера. */
    void publish(DeviceInfo deviceInfo);
}