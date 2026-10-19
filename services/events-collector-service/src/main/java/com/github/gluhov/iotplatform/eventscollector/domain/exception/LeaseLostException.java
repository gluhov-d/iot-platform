package com.github.gluhov.iotplatform.eventscollector.domain.exception;

// Аренда публикатора потеряна посреди итерации: публиковать и записывать версии outbox дальше нельзя
public class LeaseLostException extends RuntimeException {
    public LeaseLostException(String message) {
        super(message);
    }
}
