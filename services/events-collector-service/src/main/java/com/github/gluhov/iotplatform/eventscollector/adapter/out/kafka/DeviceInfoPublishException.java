package com.github.gluhov.iotplatform.eventscollector.adapter.out.kafka;

public class DeviceInfoPublishException extends RuntimeException {
    public DeviceInfoPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
