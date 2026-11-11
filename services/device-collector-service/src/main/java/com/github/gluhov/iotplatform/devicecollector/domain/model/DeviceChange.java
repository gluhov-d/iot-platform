package com.github.gluhov.iotplatform.devicecollector.domain.model;

import java.time.Instant;

/**
 * Сообщение DeviceInfo в терминах реестра.
 *
 * @param eventKey ключ идемпотентности: eventId, а у сообщений версии 1.0.0 (без eventId) - "emitted:emittedAtMs"
 * @param eventId  исходный eventId или null
 * @param eventAt  время исходного события (eventTimeMs, иначе emittedAtMs): по нему отсекаются устаревшие события
 * @param state    снимок устройства или null (сообщение 1.0.0, событие без снимка)
 */
public record DeviceChange(String deviceId,
                           String eventKey,
                           String eventId,
                           ChangeType type,
                           Instant eventAt,
                           Instant emittedAt,
                           DeviceState state,
                           MessagePosition position) {

    /** Длина deviceId ограничена: ключ шардирования и первичный ключ, попадает в логи и метки. */
    public static final int MAX_DEVICE_ID_LENGTH = 128;

    public boolean hasState() {
        return state != null;
    }

    /** Событие задаёт состояние устройства: снимок или удаление. Сообщение 1.0.0 и событие без снимка - нет. */
    public boolean changesState() {
        return hasState() || deletes();
    }

    /** Удаление: тип DELETED или снимок со статусом DELETED. */
    public boolean deletes() {
        return type == ChangeType.DELETED || (state != null && state.deleted());
    }
}