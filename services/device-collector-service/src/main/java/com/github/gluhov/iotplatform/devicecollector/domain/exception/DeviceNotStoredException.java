package com.github.gluhov.iotplatform.devicecollector.domain.exception;

import com.github.gluhov.iotplatform.devicecollector.domain.model.StorageFailureReason;

/** Устройство не записано: транзакция на шарде не выполнена. Повторы, затем devices-dlt. */
public class DeviceNotStoredException extends RuntimeException {
    private final String shard;
    private final StorageFailureReason reason;

    public DeviceNotStoredException(String deviceId, String shard, StorageAccessException cause) {
        super("Device " + deviceId + " not stored (shard " + shard + ", " + cause.reason().label() + "): "
                + cause.getMessage(), cause);
        this.shard = shard;
        this.reason = cause.reason();
    }

    public String shard() {
        return shard;
    }

    public StorageFailureReason reason() {
        return reason;
    }
}