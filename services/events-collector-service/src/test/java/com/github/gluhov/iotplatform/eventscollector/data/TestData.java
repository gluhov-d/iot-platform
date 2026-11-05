package com.github.gluhov.iotplatform.eventscollector.data;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.events.avro.DeviceSnapshot;
import com.github.gluhov.iotplatform.events.avro.DeviceStatus;
import com.github.gluhov.iotplatform.events.avro.SensorSnapshot;
import com.github.gluhov.iotplatform.events.avro.SensorStatus;

import java.time.Instant;
import java.util.List;
import java.util.Map;
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

    // Полный снимок: по нему device-collector-service создаёт устройство в реестре
    public static DeviceSnapshot snapshot(String deviceId, DeviceStatus status) {
        return DeviceSnapshot.newBuilder()
                .setDeviceId(deviceId)
                .setTenantId("tenant-1")
                .setOwnerId("owner-1")
                .setDeviceType("thermostat")
                .setSerialNumber("SN-" + deviceId)
                .setFirmwareVersion("1.4.2")
                .setStatus(status)
                .setRegisteredAtMs(NOW.minusSeconds(86_400).toEpochMilli())
                .setSettings(Map.of("mode", "eco"))
                .setSensors(List.of(SensorSnapshot.newBuilder()
                        .setSensorId("t1")
                        .setSensorType("temperature")
                        .setUnit("C")
                        .setValue("21.5")
                        .setStatus(SensorStatus.OK)
                        .build()))
                .setAttributes(Map.of("floor", "3"))
                .build();
    }

    public static DeviceEvent eventWithSnapshot(String eventId, String deviceId, long timestampMs, DeviceStatus status) {
        DeviceEvent event = event(eventId, deviceId, timestampMs);
        event.setEventType("DEVICE_STATE");
        event.setSourceSystem("gateway-7");
        event.setDevice(snapshot(deviceId, status));
        return event;
    }

    public static DeviceEvent validEvent() {
        return event(EVENT_ID, DEVICE_ID, NOW.toEpochMilli() - 1_000);
    }

    public static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}