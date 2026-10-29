package com.github.gluhov.iotplatform.eventscollector.domain.exception;

public class EventPersistenceException extends RuntimeException {

    public EventPersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
