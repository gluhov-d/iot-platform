package com.github.gluhov.iotplatform.eventscollector.domain.model;

import java.time.Instant;

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
        TraceRef traceRef) {

    public static final long INITIAL_VERSION = 1L;
    public static final int MAX_ERROR_LENGTH = 1000;

    public static OutboxRecord initial(String eventId, String deviceId, long sourceTimestampMs, Instant now, TraceRef traceRef) {
        return new OutboxRecord(eventId, deviceId, sourceTimestampMs, now, now, INITIAL_VERSION, OutboxStatus.NEW, 0, "", traceRef);
    }

    public OutboxRecord sent(Instant now) {
        return new OutboxRecord(eventId, deviceId, sourceTimestampMs, createdAt, now,
                stateVersion + 1, OutboxStatus.SENT, attempts, "", traceRef);
    }

    public OutboxRecord failed(Instant now, String error) {
        return new OutboxRecord(eventId, deviceId, sourceTimestampMs, createdAt, now,
                stateVersion + 1, OutboxStatus.FAILED, attempts + 1, truncate(error), traceRef);
    }

    private static String truncate(String error) {
        if (error == null) {
            return "";
        }
        return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH);
    }
}