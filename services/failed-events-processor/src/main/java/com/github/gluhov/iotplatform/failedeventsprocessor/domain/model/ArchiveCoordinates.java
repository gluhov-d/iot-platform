package com.github.gluhov.iotplatform.failedeventsprocessor.domain.model;

import java.time.Instant;

/** Координаты записи в failed-events и время её архивации: служебный блок документа. */
public record ArchiveCoordinates(String topic, int partition, long offset, Instant archivedAt) {
}
