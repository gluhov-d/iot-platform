package com.github.gluhov.iotplatform.devicecollector.domain.exception;

import com.github.gluhov.iotplatform.devicecollector.domain.model.StorageFailureReason;

/** Ошибка доступа к хранилищу (порт DeviceRepository / TransactionRunner): транзакция не выполнена. */
public class StorageAccessException extends RuntimeException {
    private final StorageFailureReason reason;

    public StorageAccessException(StorageFailureReason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public StorageFailureReason reason() {
        return reason;
    }
}