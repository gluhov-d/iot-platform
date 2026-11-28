package com.github.gluhov.iotplatform.failedeventsprocessor.domain.model;

import java.time.Instant;
import java.util.List;

/**
 * Запись failed-events, которую не удалось разобрать как FailedEvent: исходные байты без изменений,
 * причина и координаты самой записи в failed-events. Ключ объекта строится из времени записи (FR-010).
 */
public record UnreadableRecord(
        String topic,
        int partition,
        long offset,
        Instant recordTimestamp,
        String reason,
        byte[] key,
        byte[] value,
        List<HeaderEntry> headers) {
}
