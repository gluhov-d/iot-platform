package com.github.gluhov.iotplatform.devicecollector.domain.model;

import java.time.Instant;

/**
 * Новое состояние строки devices после применения события.
 *
 * @param state     снимок устройства или null - тогда поля модели не меняются (команда DELETE, событие без снимка)
 * @param status    статус строки после применения
 * @param deletedAt время удаления или null
 * @param deletedBy DEVICE или ADMIN при удалении, иначе null (или прежнее значение строки - решает правило)
 * @param stateAt   время события, давшего состояние (снимок или удаление роутером), или null - last_state_at не меняется
 * @param managedAt время команды администратора (для команд) или null
 */
public record DeviceRecord(String deviceId,
                           DeviceState state,
                           String status,
                           Instant lastSeenAt,
                           Instant deletedAt,
                           String deletedBy,
                           Instant eventAt,
                           String eventId,
                           ChangeType eventType,
                           Instant stateAt,
                           Instant managedAt) {}
