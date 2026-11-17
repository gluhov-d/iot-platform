package com.github.gluhov.iotplatform.deviceservice.domain.exception;

/** Устройства нет, оно удалено или у него нет карточки (для клиента - 404 DEVICE_NOT_FOUND). */
public class DeviceNotFoundException extends RuntimeException {

    private final String deviceId;

    public DeviceNotFoundException(String deviceId) {
        super("Device " + deviceId + " not found");
        this.deviceId = deviceId;
    }

    public String getDeviceId() {
        return deviceId;
    }
}
