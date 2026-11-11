package com.github.gluhov.iotplatform.devicecollector.domain.model;

import java.time.Instant;

/**
 * Новое состояние строки devices после применения события.
 *
 * @param state   снимок устройства или null - тогда поля модели не меняются
 * @param stateAt время события, давшего состояние (снимок или удаление), или null - last_state_at не меняется
 */
public record DeviceRecord(String deviceId,
                           DeviceState state,
                           String status,
                           Instant lastSeenAt,
                           Instant deletedAt,
                           Instant eventAt,
                           String eventId,
                           ChangeType eventType,
                           Instant stateAt) {}