package com.github.gluhov.iotplatform.eventscollector.domain.model;

import java.time.Instant;

/**
 * Версия записи outbox.
 *
 * @param deviceSnapshot       снимок устройства из исходного события (Avro JSON); пустая строка - событие без снимка
 * @param deviceSnapshotSchema схема Avro, которой записан снимок; пустая строка - текущая схема сервиса
 */
public record OutboxRecord(
        String eventId,
        String deviceId,
        long sourceTimestampMs,
        Instant createdAt,
        Instant updatedAt,
        long stateVersion,
        OutboxStatus status,
        int attempts,
        String lastError,
        TraceRef traceRef,
        String deviceSnapshot,
        String deviceSnapshotSchema) {

    public static final long INITIAL_VERSION = 1L;
    public static final int MAX_ERROR_LENGTH = 1000;
    public static final String NO_SNAPSHOT = "";

    public OutboxRecord {
        deviceSnapshot = deviceSnapshot == null ? NO_SNAPSHOT : deviceSnapshot;
        deviceSnapshotSchema = deviceSnapshotSchema == null || deviceSnapshot.isEmpty() ? "" : deviceSnapshotSchema;
    }

    public static OutboxRecord initial(String eventId, String deviceId, long sourceTimestampMs, Instant now, TraceRef traceRef) {
        return initial(eventId, deviceId, sourceTimestampMs, now, traceRef, NO_SNAPSHOT, "");
    }

    public static OutboxRecord initial(String eventId, String deviceId, long sourceTimestampMs, Instant now, TraceRef traceRef,
                                       String deviceSnapshot) {
        return initial(eventId, deviceId, sourceTimestampMs, now, traceRef, deviceSnapshot, "");
    }

    public static OutboxRecord initial(String eventId, String deviceId, long sourceTimestampMs, Instant now, TraceRef traceRef,
                                       String deviceSnapshot, String deviceSnapshotSchema) {
        return new OutboxRecord(eventId, deviceId, sourceTimestampMs, now, now, INITIAL_VERSION, OutboxStatus.NEW, 0, "",
                traceRef, deviceSnapshot, deviceSnapshotSchema);
    }

    public boolean hasSnapshot() {
        return !deviceSnapshot.isEmpty();
    }

    public OutboxRecord sent(Instant now) {
        return new OutboxRecord(eventId, deviceId, sourceTimestampMs, createdAt, now,
                stateVersion + 1, OutboxStatus.SENT, attempts, "", traceRef, deviceSnapshot, deviceSnapshotSchema);
    }

    public OutboxRecord failed(Instant now, String error) {
        return new OutboxRecord(eventId, deviceId, sourceTimestampMs, createdAt, now,
                stateVersion + 1, OutboxStatus.FAILED, attempts + 1, truncate(error), traceRef, deviceSnapshot,
                deviceSnapshotSchema);
    }

    private static String truncate(String error) {
        if (error == null) {
            return "";
        }
        return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
    }
}