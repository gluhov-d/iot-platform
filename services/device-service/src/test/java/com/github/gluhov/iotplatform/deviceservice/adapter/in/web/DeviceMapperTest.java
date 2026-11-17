package com.github.gluhov.iotplatform.deviceservice.adapter.in.web;

import com.github.gluhov.iotplatform.deviceservice.api.model.DeviceResponse;
import com.github.gluhov.iotplatform.deviceservice.api.model.DeviceStatus;
import com.github.gluhov.iotplatform.deviceservice.domain.model.Device;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceSensor;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DeviceMapperTest {

    private final DeviceMapper mapper = new DeviceMapperImpl();

    @Test
    void givenFullDevice_whenMapped_thenEveryContractFieldFilled() {
        Instant registered = Instant.parse("2026-01-01T10:00:00Z");
        Instant seen = Instant.parse("2026-01-02T11:30:00Z");
        Device device = new Device("dev-1", "Датчик", "t1", "owner-1", "sensor", "SN-1", "acme", "m-1", "ACTIVE",
                Map.of("interval", "60"), Map.of("room", "101"), "1.2.3", "linux", "6.1",
                List.of(new DeviceSensor("s1", "temp", "C", "21.5", Instant.parse("2026-01-02T11:29:00Z"), "OK")),
                registered, seen, 42L);

        DeviceResponse response = mapper.toResponse(device);

        assertThat(response.getDeviceId()).isEqualTo("dev-1");
        assertThat(response.getName()).isEqualTo("Датчик");
        assertThat(response.getTenantId()).isEqualTo("t1");
        assertThat(response.getOwnerId()).isEqualTo("owner-1");
        assertThat(response.getDeviceType()).isEqualTo("sensor");
        assertThat(response.getSerialNumber()).isEqualTo("SN-1");
        assertThat(response.getManufacturer()).isEqualTo("acme");
        assertThat(response.getModel()).isEqualTo("m-1");
        assertThat(response.getStatus()).isEqualTo(DeviceStatus.ACTIVE);
        assertThat(response.getSettings()).containsExactlyEntriesOf(Map.of("interval", "60"));
        assertThat(response.getAttributes()).containsExactlyEntriesOf(Map.of("room", "101"));
        assertThat(response.getFirmwareVersion()).isEqualTo("1.2.3");
        assertThat(response.getOsName()).isEqualTo("linux");
        assertThat(response.getOsVersion()).isEqualTo("6.1");
        assertThat(response.getRegisteredAt()).isEqualTo(registered.atOffset(ZoneOffset.UTC));
        assertThat(response.getLastSeenAt()).isEqualTo(seen.atOffset(ZoneOffset.UTC));
        assertThat(response.getVersion()).isEqualTo(42L);
    }

    @Test
    void givenSensors_whenMapped_thenEachSensorConvertedWithOffsetTime() {
        Instant measured = Instant.parse("2026-01-02T11:29:00Z");
        Device device = new Device("dev-2", null, "t1", null, "sensor", null, null, null, "NEW",
                Map.of(), Map.of(), null, null, null,
                List.of(new DeviceSensor("s1", "temp", "C", "21", measured, "OK")),
                null, null, 1L);

        DeviceResponse response = mapper.toResponse(device);

        assertThat(response.getSensors()).hasSize(1);
        assertThat(response.getSensors().get(0).getSensorId()).isEqualTo("s1");
        assertThat(response.getSensors().get(0).getMeasuredAt())
                .isEqualTo(OffsetDateTime.ofInstant(measured, ZoneOffset.UTC));
    }
}
