package com.github.gluhov.iotplatform.eventscollector.service.domain.service;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.InvalidEventException;
import com.github.gluhov.iotplatform.eventscollector.domain.service.DeviceEventValidator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.ZoneOffset;

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
}
