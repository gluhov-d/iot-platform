package com.github.gluhov.iotplatform.failedeventsprocessor.domain.model;

import java.time.Instant;

/**
 * Координаты записи в failed-events (тема, раздел, смещение, время записи) и время её архивации. Из первых четырёх
 * значений строится ключ объекта (ArchiveKey); все пять входят в служебный блок документа.
 */
public record ArchiveCoordinates(String topic, int partition, long offset, Instant recordTimestamp, Instant archivedAt) {
}
