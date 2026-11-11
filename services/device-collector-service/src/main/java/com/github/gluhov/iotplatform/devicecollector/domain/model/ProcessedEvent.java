package com.github.gluhov.iotplatform.devicecollector.domain.model;

import java.time.Instant;

/** Строка журнала processed_device_events. */
public record ProcessedEvent(String deviceId,
                             String eventKey,
                             ChangeType eventType,
                             Instant eventAt,
                             Instant emittedAt,
                             ProcessingOutcome outcome,
                             MessagePosition position) {

    public static ProcessedEvent of(DeviceChange change, ProcessingOutcome outcome) {
        return new ProcessedEvent(change.deviceId(), change.eventKey(), change.type(), change.eventAt(),
                change.emittedAt(), outcome, change.position());
    }
}