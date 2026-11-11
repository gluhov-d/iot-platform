package com.github.gluhov.iotplatform.eventscollector.service.domain.service;

import com.github.gluhov.iotplatform.events.avro.DeviceSnapshot;
import com.github.gluhov.iotplatform.events.avro.DeviceStatus;
import com.github.gluhov.iotplatform.eventscollector.domain.service.DeviceSnapshotJson;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.github.gluhov.iotplatform.eventscollector.data.TestData.DEVICE_ID;
import static com.github.gluhov.iotplatform.eventscollector.data.TestData.snapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeviceSnapshotJsonTest {

    @Test
    @DisplayName("Snapshot to Avro JSON and back then equal snapshot")
    void givenSnapshot_whenRoundTrip_thenEqual() {
        DeviceSnapshot snapshot = snapshot(DEVICE_ID, DeviceStatus.MAINTENANCE);

        String json = DeviceSnapshotJson.toJson(snapshot);

        assertThat(json).contains("\"deviceId\":\"" + DEVICE_ID + "\"").contains("MAINTENANCE");
        assertThat(DeviceSnapshotJson.fromJson(json)).isEqualTo(snapshot);
    }

    @Test
    @DisplayName("No snapshot then empty string and back to null")
    void givenNoSnapshot_whenConvert_thenEmptyAndNull() {
        assertThat(DeviceSnapshotJson.toJson(null)).isEmpty();
        assertThat(DeviceSnapshotJson.fromJson("")).isNull();
        assertThat(DeviceSnapshotJson.fromJson(null)).isNull();
    }

    @Test
    @DisplayName("Broken stored snapshot then IllegalArgumentException (data defect, not retried)")
    void givenBrokenJson_whenFromJson_thenException() {
        assertThatThrownBy(() -> DeviceSnapshotJson.fromJson("{\"deviceId\":"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Snapshot written by an older schema then read with schema resolution: missing field gets its default")
    void givenSnapshotOfOlderSchema_whenFromJsonWithWriterSchema_thenDefaultsApplied() throws Exception {
        // «Старая версия контракта» - текущая схема без поля model (у него default null)
        Schema current = DeviceSnapshot.getClassSchema();
        List<Schema.Field> fields = current.getFields().stream()
                .filter(f -> !f.name().equals("model"))
                .map(f -> new Schema.Field(f, f.schema()))
                .toList();
        Schema older = Schema.createRecord(current.getName(), current.getDoc(), current.getNamespace(), false, fields);
        DeviceSnapshot snapshot = snapshot(DEVICE_ID, DeviceStatus.ACTIVE);
        GenericRecord old = new GenericData.Record(older);
        older.getFields().forEach(f -> old.put(f.name(), snapshot.get(f.name())));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(older, out);
        new SpecificDatumWriter<GenericRecord>(older).write(old, encoder);
        encoder.flush();
        String json = out.toString(StandardCharsets.UTF_8);

        DeviceSnapshot read = DeviceSnapshotJson.fromJson(json, older.toString());

        assertThat(read.getDeviceId()).isEqualTo(DEVICE_ID);
        assertThat(read.getModel()).isNull();
        assertThat(read.getSensors()).isEqualTo(snapshot.getSensors());
        // Без схемы писателя Avro JSON старой версии не читается - поэтому она хранится в outbox
        assertThatThrownBy(() -> DeviceSnapshotJson.fromJson(json)).isInstanceOf(IllegalArgumentException.class);
    }
}