package com.github.gluhov.iotplatform.eventscollector.service.domain.service;

import com.github.gluhov.iotplatform.eventscollector.domain.service.DeviceEventMapper;
import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.events.avro.DeviceStatus;
import com.github.gluhov.iotplatform.eventscollector.domain.model.DeviceEventRecord;
import com.github.gluhov.iotplatform.eventscollector.domain.model.OutboxRecord;
import com.github.gluhov.iotplatform.eventscollector.domain.model.TraceRef;
import com.github.gluhov.iotplatform.eventscollector.domain.service.DeviceSnapshotJson;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;

import static com.github.gluhov.iotplatform.eventscollector.data.TestData.*;
import static org.assertj.core.api.Assertions.assertThat;

class DeviceEventMapperTest {
    private final DeviceEventMapper mapper = new DeviceEventMapper();

    @Test
    @DisplayName("Event near midnight then event date in UTC")
    void givenLateEveningUtc_whenEventDate_thenDateInUtc() {
        // 23:30 UTC — в Москве уже следующие сутки, но партиция должна быть по UTC
        long lateEvening = Instant.parse("2026-10-01T23:30:00Z").toEpochMilli();

        assertThat(DeviceEventMapper.eventDate(lateEvening)).isEqualTo(LocalDate.of(2026, 10, 1));
    }

    @Test
    @DisplayName("Event with snapshot then snapshot stored in event row and outbox as the same Avro JSON")
    void givenEventWithSnapshot_whenMap_thenSnapshotInEventAndOutbox() {
        DeviceEvent event = eventWithSnapshot(EVENT_ID, DEVICE_ID, NOW.toEpochMilli(), DeviceStatus.ACTIVE);
        event.setPayload("{\"raw\":1}");

        DeviceEventRecord row = mapper.toEventRecord(event, NOW);
        OutboxRecord outbox = mapper.toInitialOutbox(event, NOW, TraceRef.NONE);

        assertThat(row.deviceSnapshot()).isNotEmpty().isEqualTo(outbox.deviceSnapshot());
        assertThat(DeviceSnapshotJson.fromJson(row.deviceSnapshot())).isEqualTo(event.getDevice());
        assertThat(row.sourceSystem()).isEqualTo("gateway-7");
        assertThat(row.payload()).isEqualTo("{\"raw\":1}");
        assertThat(outbox.hasSnapshot()).isTrue();
    }

    @Test
    @DisplayName("Telemetry without snapshot then empty snapshot and source system, null payload")
    void givenEventWithoutSnapshot_whenMap_thenEmptySnapshot() {
        DeviceEvent event = validEvent();

        DeviceEventRecord row = mapper.toEventRecord(event, NOW);
        OutboxRecord outbox = mapper.toInitialOutbox(event, NOW, TraceRef.NONE);

        assertThat(row.deviceSnapshot()).isEmpty();
        assertThat(row.sourceSystem()).isEmpty();
        assertThat(row.payload()).isNull();
        assertThat(outbox.hasSnapshot()).isFalse();
    }
}