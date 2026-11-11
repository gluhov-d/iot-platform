package com.github.gluhov.iotplatform.devicecollector.service.domain.service;

import com.github.gluhov.iotplatform.devicecollector.domain.exception.InvalidDeviceInfoException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ValidationPolicy;
import com.github.gluhov.iotplatform.devicecollector.domain.service.DeviceInfoValidator;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import com.github.gluhov.iotplatform.events.avro.SensorSnapshot;
import com.github.gluhov.iotplatform.events.avro.SensorStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.github.gluhov.iotplatform.devicecollector.data.TestData.T0;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.T0_MS;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.info;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.legacy;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.snapshot;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeviceInfoValidatorTest {
    private static final String DEVICE = "dev-1";
    private final DeviceInfoValidator validator = new DeviceInfoValidator(
            Clock.fixed(T0.plusSeconds(60), ZoneOffset.UTC), new ValidationPolicy(Duration.ofDays(1)));

    @Test
    @DisplayName("Full DeviceInfo 1.1 with snapshot is valid")
    void givenFullDeviceInfo_whenValidate_thenAccepted() {
        assertThatCode(() -> validator.validate(DEVICE, info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0).build()))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("DeviceInfo 1.0 without eventId, event time and snapshot is valid")
    void givenLegacyDeviceInfo_whenValidate_thenAccepted() {
        assertThatCode(() -> validator.validate(DEVICE, legacy(DEVICE, T0))).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Tombstone (null value) is invalid")
    void givenNullValue_whenValidate_thenRejected() {
        assertThatThrownBy(() -> validator.validate(DEVICE, null))
                .isInstanceOf(InvalidDeviceInfoException.class)
                .hasMessageContaining("empty");
    }

    static Stream<Arguments> invalidMessages() {
        return Stream.of(
                invalid("blank deviceId", DEVICE, b -> b.setDeviceId(" ").setDevice(null), "deviceId is blank"),
                invalid("deviceId longer than 128", "d".repeat(129), b -> b.setDeviceId("d".repeat(129)).setDevice(null),
                        "longer than 128"),
                invalid("key differs from deviceId", "other-key", b -> b, "differs from deviceId"),
                invalid("key is null", null, b -> b, "differs from deviceId"),
                invalid("blank eventId", DEVICE, b -> b.setEventId(""), "eventId is blank"),
                invalid("emittedAtMs is zero", DEVICE, b -> b.setEmittedAtMs(0), "emittedAtMs must be positive"),
                invalid("emittedAtMs more than a day ahead", DEVICE, b -> b.setEmittedAtMs(T0_MS + Duration.ofDays(2).toMillis()),
                        "in the future"),
                invalid("eventTimeMs negative", DEVICE, b -> b.setEventTimeMs(-1L), "eventTimeMs must be positive"),
                invalid("snapshot of another device", DEVICE, b -> b.setDevice(snapshot("dev-2").build()),
                        "device.deviceId 'dev-2' differs"),
                invalid("snapshot without tenantId", DEVICE, b -> b.setDevice(snapshot(DEVICE).setTenantId(" ").build()),
                        "device.tenantId is blank"),
                invalid("snapshot without deviceType", DEVICE, b -> b.setDevice(snapshot(DEVICE).setDeviceType("").build()),
                        "device.deviceType is blank"),
                invalid("sensor without sensorId", DEVICE, b -> b.setDevice(snapshot(DEVICE)
                        .setSensors(List.of(sensor(" "))).build()), "sensorId is blank"),
                invalid("duplicate sensorId", DEVICE, b -> b.setDevice(snapshot(DEVICE)
                        .setSensors(List.of(sensor("t1"), sensor("t1"))).build()), "duplicate sensorId 't1'"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidMessages")
    @DisplayName("Invalid DeviceInfo is rejected with the reason")
    void givenInvalidDeviceInfo_whenValidate_thenRejected(String scenario, String key, DeviceInfo message, String reason) {
        assertThatThrownBy(() -> validator.validate(key, message))
                .isInstanceOf(InvalidDeviceInfoException.class)
                .hasMessageContaining(reason);
    }

    private static Arguments invalid(String scenario, String key, UnaryOperator<DeviceInfo.Builder> change, String reason) {
        return Arguments.of(scenario, key, change.apply(info(DEVICE, "evt-1", DeviceUpsertEventType.UPDATED, T0)).build(), reason);
    }

    private static SensorSnapshot sensor(String id) {
        return SensorSnapshot.newBuilder().setSensorId(id).setSensorType("TEMPERATURE").setStatus(SensorStatus.OK).build();
    }
}