package com.github.gluhov.iotplatform.eventscollector.service.domain.service;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.events.avro.DeviceStatus;
import com.github.gluhov.iotplatform.events.avro.SensorSnapshot;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.InvalidEventException;
import com.github.gluhov.iotplatform.eventscollector.domain.service.DeviceEventValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.ZoneOffset;
import java.util.List;

import static com.github.gluhov.iotplatform.eventscollector.data.TestData.*;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeviceEventValidatorTest {
    private final DeviceEventValidator validator = new DeviceEventValidator(Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    @DisplayName("Validate valid event then no exception")
    void givenValidEvent_whenValidate_thenAccept() {
        assertThatCode(() -> validator.validate(validEvent())).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Validate null event then exception")
    void givenNullEvent_whenValidate_thenThrowException() {
        assertThatThrownBy(() -> validator.validate(null))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("null");
    }

    @Test
    @DisplayName("Validate event with blank type then exception")
    void givenBlankEventType_whenValidate_thenThrowException() {
        DeviceEvent event = validEvent();
        event.setEventType(" ");

        assertThatThrownBy(() -> validator.validate(event))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("eventType");
    }

    @Test
    @DisplayName("Validate event from the future then accept small skew and reject far future")
    void givenFutureTimestamp_whenValidate_thenAcceptSkewAndRejectFarFuture() {
        long skewed = NOW.plus(DeviceEventValidator.MAX_FUTURE_SKEW).toEpochMilli();

        assertThatCode(() -> validator.validate(event("e", "d", skewed))).doesNotThrowAnyException();
        assertThatThrownBy(() -> validator.validate(event("e", "d", skewed + 1)))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("future");
    }

    @Test
    @DisplayName("Validate event with full snapshot of the same device then accept")
    void givenSnapshotOfSameDevice_whenValidate_thenAccept() {
        DeviceEvent event = eventWithSnapshot(EVENT_ID, DEVICE_ID, NOW.toEpochMilli(), DeviceStatus.ACTIVE);

        assertThatCode(() -> validator.validate(event)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Validate snapshot of another device then exception")
    void givenSnapshotOfAnotherDevice_whenValidate_thenThrowException() {
        DeviceEvent event = eventWithSnapshot(EVENT_ID, DEVICE_ID, NOW.toEpochMilli(), DeviceStatus.ACTIVE);
        event.getDevice().setDeviceId("dev-other");

        assertThatThrownBy(() -> validator.validate(event))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("device.deviceId");
    }

    @Test
    @DisplayName("Validate snapshot without tenant then exception")
    void givenSnapshotWithoutTenant_whenValidate_thenThrowException() {
        DeviceEvent event = eventWithSnapshot(EVENT_ID, DEVICE_ID, NOW.toEpochMilli(), DeviceStatus.ACTIVE);
        event.getDevice().setTenantId(" ");

        assertThatThrownBy(() -> validator.validate(event))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("device.tenantId");
    }

    @Test
    @DisplayName("Validate snapshot with duplicate sensor ids then exception")
    void givenDuplicateSensors_whenValidate_thenThrowException() {
        DeviceEvent event = eventWithSnapshot(EVENT_ID, DEVICE_ID, NOW.toEpochMilli(), DeviceStatus.ACTIVE);
        SensorSnapshot sensor = event.getDevice().getSensors().getFirst();
        event.getDevice().setSensors(List.of(sensor, SensorSnapshot.newBuilder(sensor).build()));

        assertThatThrownBy(() -> validator.validate(event))
                .isInstanceOf(InvalidEventException.class)
                .hasMessageContaining("duplicate sensorId");
    }

}
