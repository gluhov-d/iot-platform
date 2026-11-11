package com.github.gluhov.iotplatform.devicecollector.domain.model;

import java.util.Locale;

/** Причина ошибки хранилища; для ошибок доступа - метка reason в devices_shard_route_fail_total. */
public enum StorageFailureReason {
    /** Нет соединения с шардом или с Proxy (SQLState 08*). */
    CONNECTION,
    /** Запрос или ожидание блокировки прервано по таймауту (57014, 55P03). */
    TIMEOUT,
    /** Proxy не построил маршрут: нет ключа шардирования, таблица неизвестна Proxy (44000, 42S02). */
    ROUTING,
    /** Прочие ошибки SQL (например, конфликт ключа при одновременной вставке). */
    OTHER;

    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** Ошибка маршрутизации или доступа к шарду - то, что считает devices_shard_route_fail_total. */
    public boolean isShardAccess() {
        return this != OTHER;
    }
}