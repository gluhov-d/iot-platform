package com.github.gluhov.iotplatform.devicecollector.domain.port.out;

import java.util.function.Supplier;

/** Локальная транзакция хранилища: все запросы внутри - по одному device_id, то есть на одном шарде. */
public interface TransactionRunner {

    /** @throws com.github.gluhov.iotplatform.devicecollector.domain.exception.StorageAccessException транзакция откатана */
    <T> T inTransaction(Supplier<T> work);
}