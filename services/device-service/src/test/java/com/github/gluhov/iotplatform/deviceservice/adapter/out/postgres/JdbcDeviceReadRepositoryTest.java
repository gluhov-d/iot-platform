package com.github.gluhov.iotplatform.deviceservice.adapter.out.postgres;

import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceSensor;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcDeviceReadRepositoryTest {

    @Test
    void givenStatusInContract_whenNormalized_thenKept() {
        assertThat(JdbcDeviceReadRepository.normalizeStatus("ACTIVE")).isEqualTo("ACTIVE");
        assertThat(JdbcDeviceReadRepository.normalizeStatus("UNKNOWN")).isEqualTo("UNKNOWN");
    }

    @Test
    void givenStatusOutsideContract_whenNormalized_thenUnknown() {
        assertThat(JdbcDeviceReadRepository.normalizeStatus("RETIRED")).isEqualTo("UNKNOWN");
        assertThat(JdbcDeviceReadRepository.normalizeStatus(null)).isEqualTo("UNKNOWN");
    }

    @Test
    void givenSensorsJson_whenParsed_thenMeasuredAtFromMilliseconds() {
        List<DeviceSensor> sensors = JdbcDeviceReadRepository.parseSensors(
                "[{\"sensorId\":\"s1\",\"sensorType\":\"temp\",\"unit\":\"C\",\"value\":\"21\","
                        + "\"measuredAtMs\":1767225600000,\"status\":\"OK\"}]");

        assertThat(sensors).containsExactly(new DeviceSensor("s1", "temp", "C", "21",
                Instant.ofEpochMilli(1767225600000L), "OK"));
    }

    @Test
    void givenEmptySensorsOrNoTime_whenParsed_thenEmptyOrNullTime() {
        assertThat(JdbcDeviceReadRepository.parseSensors("[]")).isEmpty();
        assertThat(JdbcDeviceReadRepository.parseSensors(
                "[{\"sensorId\":\"s2\",\"sensorType\":\"hum\"}]"))
                .containsExactly(new DeviceSensor("s2", "hum", null, null, null, null));
    }
}
