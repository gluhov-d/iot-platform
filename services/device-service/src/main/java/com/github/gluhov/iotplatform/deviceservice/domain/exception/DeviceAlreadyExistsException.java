package com.github.gluhov.iotplatform.deviceservice.domain.exception;

/** Устройство уже есть и не удалено (для клиента - 409 DEVICE_ALREADY_EXISTS). */
public class DeviceAlreadyExistsException extends RuntimeException {

    private final String deviceId;

    public DeviceAlreadyExistsException(String deviceId) {
        super("Device " + deviceId + " already exists");
        this.deviceId = deviceId;
    }

    public String getDeviceId() {
        return deviceId;
    }
}
