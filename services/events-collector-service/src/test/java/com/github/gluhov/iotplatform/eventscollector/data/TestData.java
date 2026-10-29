package com.github.gluhov.iotplatform.eventscollector.data;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;

import java.time.Instant;
import java.util.UUID;

public class TestData {
    public static final Instant NOW = Instant.parse("2026-10-01T10:15:30.123Z");
    public static final String EVENT_ID = "evt-1";
    public static final String DEVICE_ID = "dev-1";
    public static final String EVENT_TYPE = "TEMPERATURE";

    private TestData() {
    }

    public static DeviceEvent event(String eventId, String deviceId, long timestampMs) {
        return DeviceEvent.newBuilder()
                .setEventId(eventId)
                .setDeviceId(deviceId)
                .setTimestampMs(timestampMs)
                .setEventType(EVENT_TYPE)
                .build();
    }

    public static DeviceEvent validEvent() {
        return event(EVENT_ID, DEVICE_ID, NOW.toEpochMilli() - 1_000);
    }

    public static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
