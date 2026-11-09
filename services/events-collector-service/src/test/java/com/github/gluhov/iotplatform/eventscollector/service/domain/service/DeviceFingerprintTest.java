package com.github.gluhov.iotplatform.eventscollector.service.domain.service;

import com.github.gluhov.iotplatform.events.avro.DeviceSnapshot;
import com.github.gluhov.iotplatform.events.avro.DeviceStatus;
import com.github.gluhov.iotplatform.events.avro.SensorSnapshot;
import com.github.gluhov.iotplatform.events.avro.SensorStatus;
import com.github.gluhov.iotplatform.eventscollector.domain.service.DeviceFingerprint;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static com.github.gluhov.iotplatform.eventscollector.data.TestData.DEVICE_ID;
import static com.github.gluhov.iotplatform.eventscollector.data.TestData.snapshot;
import static org.assertj.core.api.Assertions.assertThat;

class DeviceFingerprintTest {
    private final DeviceSnapshot base = snapshot(DEVICE_ID, DeviceStatus.ACTIVE);

    @Test
    @DisplayName("Fingerprint is 64 hex characters and the same for the same data")
    void givenSameData_whenFingerprint_thenSameHex() {
        String first = DeviceFingerprint.of(base);

        assertThat(first).matches("[0-9a-f]{64}");
        assertThat(DeviceFingerprint.of(DeviceSnapshot.newBuilder(base).build())).isEqualTo(first);
    }

    @Test
    @DisplayName("Last seen time and sensor readings do not change the fingerprint")
    void givenOtherLastSeenAndSensors_whenFingerprint_thenUnchanged() {
        DeviceSnapshot changed = DeviceSnapshot.newBuilder(base)
                .setLastSeenAtMs(123_456L)
                .setSensors(List.of(SensorSnapshot.newBuilder()
                                .setSensorId("t1").setSensorType("temperature").setUnit("C")
                                .setValue("30.1").setMeasuredAtMs(99L).setStatus(SensorStatus.WARNING).build(),
                        SensorSnapshot.newBuilder().setSensorId("h1").setSensorType("humidity").build()))
                .build();

        assertThat(DeviceFingerprint.of(changed)).isEqualTo(DeviceFingerprint.of(base));
    }

    @Test
    @DisplayName("Each device data field changes the fingerprint")
    void givenChangedField_whenFingerprint_thenDifferent() {
        String original = DeviceFingerprint.of(base);
        Map<String, UnaryOperator<DeviceSnapshot.Builder>> changes = new LinkedHashMap<>();
        changes.put("deviceId", b -> b.setDeviceId("other"));
        changes.put("tenantId", b -> b.setTenantId("tenant-2"));
        changes.put("ownerId", b -> b.setOwnerId("owner-2"));
        changes.put("deviceType", b -> b.setDeviceType("gateway"));
        changes.put("serialNumber", b -> b.setSerialNumber("SN-2"));
        changes.put("manufacturer", b -> b.setManufacturer("Acme"));
        changes.put("model", b -> b.setModel("X1"));
        changes.put("firmwareVersion", b -> b.setFirmwareVersion("1.4.3"));
        changes.put("name", b -> b.setName("Renamed"));
        changes.put("osName", b -> b.setOsName("FreeRTOS"));
        changes.put("osVersion", b -> b.setOsVersion("6.2"));
        changes.put("status", b -> b.setStatus(DeviceStatus.MAINTENANCE));
        changes.put("registeredAtMs", b -> b.setRegisteredAtMs(1L));
        changes.put("settings", b -> b.setSettings(Map.of("mode", "boost")));
        changes.put("attributes", b -> b.setAttributes(Map.of("floor", "4")));

        changes.forEach((field, change) -> assertThat(DeviceFingerprint.of(change.apply(DeviceSnapshot.newBuilder(base)).build()))
                .as("field %s", field).isNotEqualTo(original));
    }

    @Test
    @DisplayName("Order of map entries does not matter")
    void givenSameMapInOtherOrder_whenFingerprint_thenSame() {
        Map<String, String> ab = new LinkedHashMap<>();
        ab.put("a", "1");
        ab.put("b", "2");
        Map<String, String> ba = new LinkedHashMap<>();
        ba.put("b", "2");
        ba.put("a", "1");

        assertThat(DeviceFingerprint.of(DeviceSnapshot.newBuilder(base).setSettings(ab).build()))
                .isEqualTo(DeviceFingerprint.of(DeviceSnapshot.newBuilder(base).setSettings(ba).build()));
    }

    @Test
    @DisplayName("Absent value, empty string and a value moved to a neighbouring field are different data")
    void givenNullEmptyAndShiftedValues_whenFingerprint_thenDifferent() {
        String withoutName = DeviceFingerprint.of(DeviceSnapshot.newBuilder(base).setName(null).build());
        String emptyName = DeviceFingerprint.of(DeviceSnapshot.newBuilder(base).setName("").build());
        String shifted1 = DeviceFingerprint.of(DeviceSnapshot.newBuilder(base).setOsName("ab").setOsVersion("c").build());
        String shifted2 = DeviceFingerprint.of(DeviceSnapshot.newBuilder(base).setOsName("a").setOsVersion("bc").build());
        String key = DeviceFingerprint.of(DeviceSnapshot.newBuilder(base).setSettings(Map.of("a", "b")).setAttributes(Map.of()).build());
        String moved = DeviceFingerprint.of(DeviceSnapshot.newBuilder(base).setSettings(Map.of()).setAttributes(Map.of("a", "b")).build());

        assertThat(withoutName).isNotEqualTo(emptyName);
        assertThat(shifted1).isNotEqualTo(shifted2);
        assertThat(key).as("the same entry in settings and in attributes").isNotEqualTo(moved);
    }
}