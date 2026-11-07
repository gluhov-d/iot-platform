package com.github.gluhov.iotplatform.devicecollector.domain.port.in;

import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceView;

import java.util.Optional;

public interface FindDeviceUseCase {

    /** Устройство по ключу шардирования: запрос уходит ровно на один шард. */
    Optional<DeviceView> find(String deviceId);
}