package com.github.gluhov.iotplatform.eventscollector.domain.model;

import java.time.Instant;
import java.time.LocalDate;

public record DeviceEventRecord(
        String deviceId,
        String eventId,
        LocalDate eventDate,
        long timestampMs,
        String eventType,
        Instant ingestedAt) {}
