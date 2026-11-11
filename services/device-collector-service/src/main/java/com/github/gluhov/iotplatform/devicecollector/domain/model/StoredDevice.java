package com.github.gluhov.iotplatform.devicecollector.domain.model;

import java.time.Instant;

/**
 * Строка devices, заблокированная SELECT ... FOR UPDATE, и шард, который её вернул.
 *
 * @param lastEventAt самое позднее событие устройства (любого типа)
 * @param lastStateAt событие, давшее текущее состояние (снимок или удаление); null - состояния ещё не было
 */
public record StoredDevice(String shard, String status, Instant lastEventAt, Instant lastStateAt, Instant lastSeenAt,
                           Instant deletedAt) {}