package com.github.gluhov.iotplatform.devicecollector.service.domain.service;

import com.github.gluhov.iotplatform.devicecollector.domain.model.ChangeType;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceChange;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceState;
import com.github.gluhov.iotplatform.devicecollector.domain.model.SensorState;
import com.github.gluhov.iotplatform.devicecollector.domain.service.DeviceInfoMapper;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceStatus;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static com.github.gluhov.iotplatform.devicecollector.data.TestData.POSITION;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.T0;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.T0_MS;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.info;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.legacy;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.snapshot;
import static org.assertj.core.api.Assertions.assertThat;

class DeviceInfoMapperTest {
    private final DeviceInfoMapper mapper = new DeviceInfoMapper();

    @Test
    @DisplayName("emittedAtMs (Unix milliseconds) becomes Instant without losing precision")
    void givenEmittedAtMs_whenToInstant_thenSameMoment() {
        assertThat(DeviceInfoMapper.toInstant(1_762_164_000_123L)).isEqualTo(Instant.parse("2025-11-03T10:00:00.123Z"));
        assertThat(DeviceInfoMapper.toInstant(T0_MS)).isEqualTo(T0);
    }

    @Test
    @DisplayName("DeviceInfo 1.1: event key is eventId, event time is eventTimeMs, not the publication time")
    void givenDeviceInfoWithEventTime_whenMap_thenEventTimeAndIdUsed() {
        DeviceInfo message = info("dev-1", "evt-1", DeviceUpsertEventType.UPDATED, T0).build();

        DeviceChange change = mapper.toChange(message, POSITION);

        assertThat(change.deviceId()).isEqualTo("dev-1");
        assertThat(change.eventKey()).isEqualTo("evt-1");
        assertThat(change.eventId()).isEqualTo("evt-1");
        assertThat(change.type()).isEqualTo(ChangeType.UPDATED);
        assertThat(change.eventAt()).isEqualTo(T0);
        assertThat(change.emittedAt()).isEqualTo(T0.plusMillis(500));
        assertThat(change.position()).isEqualTo(POSITION);
        assertThat(change.deletes()).isFalse();
    }

    @Test
    @DisplayName("DeviceInfo 1.0: key is emittedAtMs, event time is the publication time, type SNAPSHOT, no state")
    void givenLegacyDeviceInfo_whenMap_thenEmittedAtUsed() {
        DeviceChange change = mapper.toChange(legacy("dev-1", T0), POSITION);

        assertThat(change.eventKey()).isEqualTo(DeviceInfoMapper.LEGACY_KEY_PREFIX + T0_MS);
        assertThat(change.eventId()).isNull();
        assertThat(change.eventAt()).isEqualTo(T0);
        assertThat(change.emittedAt()).isEqualTo(T0);
        assertThat(change.type()).isEqualTo(ChangeType.SNAPSHOT);
        assertThat(change.hasState()).isFalse();
    }

    @Test
    @DisplayName("Snapshot fields, sensors and maps are mapped; absent times stay null")
    void givenSnapshot_whenMap_thenFullDeviceState() {
        DeviceInfo message = info("dev-1", "evt-1", DeviceUpsertEventType.CREATED, T0)
                .setDevice(snapshot("dev-1").setRegisteredAtMs(null).build())
                .build();

        DeviceState state = mapper.toChange(message, POSITION).state();

        assertThat(state.deviceId()).isEqualTo("dev-1");
        assertThat(state.tenantId()).isEqualTo("tenant-1");
        assertThat(state.ownerId()).isEqualTo("owner-1");
        assertThat(state.deviceType()).isEqualTo("thermometer");
        assertThat(state.serialNumber()).isEqualTo("SN-001");
        assertThat(state.manufacturer()).isEqualTo("Acme");
        assertThat(state.model()).isEqualTo("T-1000");
        assertThat(state.firmwareVersion()).isEqualTo("1.0.0");
        assertThat(state.status()).isEqualTo("ACTIVE");
        assertThat(state.registeredAt()).isNull();
        assertThat(state.lastSeenAt()).isEqualTo(T0);
        assertThat(state.settings()).isEqualTo(Map.of("mode", "eco"));
        assertThat(state.attributes()).isEqualTo(Map.of("floor", "3"));
        assertThat(state.sensors()).containsExactly(new SensorState("t1", "TEMPERATURE", "C", "21.5", T0, "OK"));
    }

    @Test
    @DisplayName("Snapshot with status DELETED deletes the device even if the event type is UPDATED")
    void givenDeletedSnapshot_whenMap_thenChangeDeletes() {
        DeviceInfo message = info("dev-1", "evt-1", DeviceUpsertEventType.UPDATED, T0)
                .setDevice(snapshot("dev-1").setStatus(DeviceStatus.DELETED).build())
                .build();

        assertThat(mapper.toChange(message, POSITION).deletes()).isTrue();
    }
}