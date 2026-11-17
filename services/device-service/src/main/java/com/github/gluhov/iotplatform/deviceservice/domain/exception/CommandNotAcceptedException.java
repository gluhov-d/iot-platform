package com.github.gluhov.iotplatform.deviceservice.domain.exception;

/**
 * Kafka не подтвердила команду: команда могла не записаться, а могла записаться позже (повтор доставки продюсера).
 * Для клиента - 503 COMMAND_NOT_ACCEPTED.
 */
public class CommandNotAcceptedException extends RuntimeException {

    private final String deviceId;

    public CommandNotAcceptedException(String deviceId, Throwable cause) {
        super("Command for device " + deviceId + " was not accepted by Kafka", cause);
        this.deviceId = deviceId;
    }

    public String getDeviceId() {
        return deviceId;
    }
}
