package com.github.gluhov.iotplatform.deviceservice.domain.exception;

/** Модель чтения (реплики через ShardingSphere-Proxy) недоступна при промахе кэша (для клиента - 503 READ_MODEL_UNAVAILABLE). */
public class ReadModelUnavailableException extends RuntimeException {

    public ReadModelUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
