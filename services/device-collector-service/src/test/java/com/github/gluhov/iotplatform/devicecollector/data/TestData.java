package com.github.gluhov.iotplatform.devicecollector.data;

import com.github.gluhov.iotplatform.devicecollector.domain.model.MessagePosition;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceSnapshot;
import com.github.gluhov.iotplatform.events.avro.DeviceStatus;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import com.github.gluhov.iotplatform.events.avro.SensorSnapshot;
import com.github.gluhov.iotplatform.events.avro.SensorStatus;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Тестовые DeviceInfo и снимки устройства. */
public final class TestData {
    /** Время событий в тестах; часы валидатора - на минуту позже. */
    public static final Instant T0 = Instant.parse("2026-11-03T10:00:00Z");
    public static final long T0_MS = T0.toEpochMilli();
    public static final MessagePosition POSITION = new MessagePosition("devices", 1, 42L);

    private TestData() {
    }

    public static String unique(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    public static DeviceSnapshot.Builder snapshot(String deviceId) {
        return DeviceSnapshot.newBuilder()
                .setDeviceId(deviceId)
                .setTenantId("tenant-1")
                .setOwnerId("owner-1")
                .setDeviceType("thermometer")
                .setSerialNumber("SN-001")
                .setManufacturer("Acme")
                .setModel("T-1000")
                .setFirmwareVersion("1.0.0")
                .setStatus(DeviceStatus.ACTIVE)
                .setRegisteredAtMs(T0_MS - 86_400_000L)
                .setLastSeenAtMs(T0_MS)
                .setSettings(Map.of("mode", "eco"))
                .setAttributes(Map.of("floor", "3"))
                .setSensors(List.of(SensorSnapshot.newBuilder()
                        .setSensorId("t1")
                        .setSensorType("TEMPERATURE")
                        .setUnit("C")
                        .setValue("21.5")
                        .setMeasuredAtMs(T0_MS)
                        .setStatus(SensorStatus.OK)
                        .build()));
    }

    /** Событие версии 1.1.0: eventId, время события и снимок. */
    public static DeviceInfo.Builder info(String deviceId, String eventId, DeviceUpsertEventType type, Instant eventTime) {
        return DeviceInfo.newBuilder()
                .setDeviceId(deviceId)
                .setEventId(eventId)
                .setEventType(type)
                .setEventTimeMs(eventTime.toEpochMilli())
                .setEmittedAtMs(eventTime.toEpochMilli() + 500)
                .setDevice(snapshot(deviceId).build());
    }

    /** Сообщение версии 1.0.0: только deviceId и emittedAtMs. */
    public static DeviceInfo legacy(String deviceId, Instant emittedAt) {
        return DeviceInfo.newBuilder()
                .setDeviceId(deviceId)
                .setEmittedAtMs(emittedAt.toEpochMilli())
                .build();
    }
}