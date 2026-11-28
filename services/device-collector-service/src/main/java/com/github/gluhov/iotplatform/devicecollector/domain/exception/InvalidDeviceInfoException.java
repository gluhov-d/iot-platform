package com.github.gluhov.iotplatform.devicecollector.domain.exception;

/** Сообщение нельзя обработать никогда: повтор не поможет, сразу в failed-events. */
public class InvalidDeviceInfoException extends RuntimeException {

    public InvalidDeviceInfoException(String message) {
        super(message);
    }
}