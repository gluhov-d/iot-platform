package com.github.gluhov.iotplatform.deviceservice.domain.exception;

/** Кэш устройств недоступен: чтение продолжается из модели чтения (FR-021). */
public class CacheUnavailableException extends RuntimeException {

    public CacheUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
