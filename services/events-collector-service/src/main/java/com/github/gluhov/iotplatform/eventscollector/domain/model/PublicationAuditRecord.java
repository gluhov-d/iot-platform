package com.github.gluhov.iotplatform.eventscollector.domain.model;

import java.time.Instant;

public record PublicationAuditRecord(String eventId, String deviceId, PublishOutcome outcome, int attempt,
                                     String error, Instant occurredAt) {

    // attempts в outbox считает только неудачные попытки до этой, поэтому номер текущей - attempts + 1
    public static PublicationAuditRecord of(OutboxRecord record, PublishOutcome outcome, String error, Instant now) {
        String text = error == null ? "" : error.substring(0, Math.min(error.length(), OutboxRecord.MAX_ERROR_LENGTH));
        return new PublicationAuditRecord(record.eventId(), record.deviceId(), outcome, record.attempts() + 1, text, now);
    }
}