package com.github.gluhov.iotplatform.devicecollector.domain.model;

import java.time.Instant;

/**
 * Строка devices, заблокированная SELECT ... FOR UPDATE, и шард, который её вернул.
 *
 * @param version     версия записи: растёт на 1 при каждом APPLIED
 * @param lastEventAt самое позднее событие устройства (любого типа)
 * @param lastStateAt событие, давшее текущее состояние (снимок или удаление роутером); null - состояния ещё не было
 * @param deletedAt   время удаления или null
 * @param deletedBy   кто удалил: DEVICE (событие роутера), ADMIN (команда) или null
 * @param managedAt   время последней применённой команды администратора или null - устройство не управлялось
 */
public record StoredDevice(String shard,
                           long version,
                           String status,
                           Instant lastEventAt,
                           Instant lastStateAt,
                           Instant lastSeenAt,
                           Instant deletedAt,
                           String deletedBy,
                           Instant managedAt) {

    public static final String DELETED_BY_DEVICE = "DEVICE";
    public static final String DELETED_BY_ADMIN = "ADMIN";

    /** Удалено администратором (команда DELETE): события роутера не возвращают устройство. */
    public boolean deletedByAdmin() {
        return deletedAt != null && DELETED_BY_ADMIN.equals(deletedBy);
    }

    /** Есть хотя бы одна применённая команда администратора. */
    public boolean managed() {
        return managedAt != null;
    }
}
