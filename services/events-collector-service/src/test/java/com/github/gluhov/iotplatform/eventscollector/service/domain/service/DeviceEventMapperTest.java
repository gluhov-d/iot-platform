package com.github.gluhov.iotplatform.eventscollector.service.domain.service;

import com.github.gluhov.iotplatform.eventscollector.domain.service.DeviceEventMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

class DeviceEventMapperTest {

    @Test
    @DisplayName("Event near midnight then event date in UTC")
    void givenLateEveningUtc_whenEventDate_thenDateInUtc() {
        // 23:30 UTC — в Москве уже следующие сутки, но партиция должна быть по UTC
        long lateEvening = Instant.parse("2026-10-01T23:30:00Z").toEpochMilli();

        assertThat(DeviceEventMapper.eventDate(lateEvening)).isEqualTo(LocalDate.of(2026, 10, 1));
    }
}
