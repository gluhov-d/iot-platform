package com.github.gluhov.iotplatform.eventscollector.service.contract;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceSnapshot;
import com.github.gluhov.iotplatform.events.avro.DeviceStatus;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import static com.github.gluhov.iotplatform.eventscollector.data.TestData.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Сообщения контрактов 1.0.0, 1.1.0 и 1.2.0 читаются в обе стороны: сервисы переходят на новую версию независимо.
 * Схемы 1.0.0 - как в теге contracts/v1.0.0.
 */
class ContractCompatibilityTest {
    private static final Schema DEVICE_INFO_1_0 = new Schema.Parser().parse("""
            {"type":"record","name":"DeviceInfo","namespace":"com.github.gluhov.iotplatform.events.avro",
             "fields":[{"name":"deviceId","type":"string"},{"name":"emittedAtMs","type":"long"}]}
            """);
    private static final Schema DEVICE_EVENT_1_0 = new Schema.Parser().parse("""
            {"type":"record","name":"DeviceEvent","namespace":"com.github.gluhov.iotplatform.events.avro",
             "fields":[{"name":"eventId","type":"string"},{"name":"deviceId","type":"string"},
                       {"name":"timestampMs","type":"long"},{"name":"eventType","type":"string"}]}
            """);

    @Test
    @DisplayName("DeviceInfo 1.0.0 read by 1.1.0 then SNAPSHOT without eventId and device")
    void givenOldDeviceInfo_whenReadWithNewSchema_thenDefaults() throws IOException {
        GenericRecord old = new GenericData.Record(DEVICE_INFO_1_0);
        old.put("deviceId", DEVICE_ID);
        old.put("emittedAtMs", 123L);

        DeviceInfo read = new SpecificDatumReader<DeviceInfo>(DEVICE_INFO_1_0, DeviceInfo.getClassSchema())
                .read(null, decoder(writeGeneric(old)));

        assertThat(read.getDeviceId()).isEqualTo(DEVICE_ID);
        assertThat(read.getEmittedAtMs()).isEqualTo(123L);
        assertThat(read.getEventType()).isEqualTo(DeviceUpsertEventType.SNAPSHOT);
        assertThat(read.getEventId()).isNull();
        assertThat(read.getEventTimeMs()).isNull();
        assertThat(read.getDevice()).isNull();
    }

    @Test
    @DisplayName("DeviceInfo 1.1.0 with device read by 1.0.0 then deviceId and emittedAtMs, new fields ignored")
    void givenNewDeviceInfo_whenReadWithOldSchema_thenKnownFields() throws IOException {
        DeviceInfo current = DeviceInfo.newBuilder()
                .setDeviceId(DEVICE_ID)
                .setEmittedAtMs(456L)
                .setEventType(DeviceUpsertEventType.UPDATED)
                .setEventId(EVENT_ID)
                .setEventTimeMs(400L)
                .setDevice(snapshot(DEVICE_ID, DeviceStatus.ACTIVE))
                .build();

        GenericRecord read = new GenericDatumReader<GenericRecord>(DeviceInfo.getClassSchema(), DEVICE_INFO_1_0)
                .read(null, decoder(writeSpecific(current, DeviceInfo.getClassSchema())));

        assertThat(read.get("deviceId").toString()).isEqualTo(DEVICE_ID);
        assertThat(read.get("emittedAtMs")).isEqualTo(456L);
        assertThat(read.getSchema().getFields()).hasSize(2);
    }

    @Test
    @DisplayName("DeviceEvent 1.0.0 read by 1.1.0 then telemetry without snapshot")
    void givenOldDeviceEvent_whenReadWithNewSchema_thenNoSnapshot() throws IOException {
        GenericRecord old = new GenericData.Record(DEVICE_EVENT_1_0);
        old.put("eventId", EVENT_ID);
        old.put("deviceId", DEVICE_ID);
        old.put("timestampMs", 789L);
        old.put("eventType", EVENT_TYPE);

        DeviceEvent read = new SpecificDatumReader<DeviceEvent>(DEVICE_EVENT_1_0, DeviceEvent.getClassSchema())
                .read(null, decoder(writeGeneric(old)));

        assertThat(read.getEventId()).isEqualTo(EVENT_ID);
        assertThat(read.getDevice()).isNull();
        assertThat(read.getSourceSystem()).isNull();
        assertThat(read.getPayload()).isNull();
    }

    /** Снимок 1.1.0 - текущий без полей, добавленных в 1.2.0 (name, osName, osVersion). */
    private static final Schema SNAPSHOT_1_1 = withoutFields(DeviceSnapshot.getClassSchema(),
            Set.of("name", "osName", "osVersion"));

    @Test
    @DisplayName("DeviceSnapshot 1.1.0 read by 1.2.0 then name and OS are null, other fields kept")
    void givenSnapshot11_whenReadWithNewSchema_thenNewFieldsNull() throws IOException {
        DeviceSnapshot current = snapshot(DEVICE_ID, DeviceStatus.ACTIVE);
        GenericRecord old = new GenericData.Record(SNAPSHOT_1_1);
        SNAPSHOT_1_1.getFields().forEach(f -> old.put(f.name(), current.get(f.name())));

        DeviceSnapshot read = new SpecificDatumReader<DeviceSnapshot>(SNAPSHOT_1_1, DeviceSnapshot.getClassSchema())
                .read(null, decoder(writeGeneric(old)));

        assertThat(read.getDeviceId()).isEqualTo(DEVICE_ID);
        assertThat(read.getFirmwareVersion()).isEqualTo(current.getFirmwareVersion());
        assertThat(read.getName()).isNull();
        assertThat(read.getOsName()).isNull();
        assertThat(read.getOsVersion()).isNull();
    }

    @Test
    @DisplayName("DeviceSnapshot 1.2.0 read by 1.1.0 then new fields ignored")
    void givenSnapshot12_whenReadWithOldSchema_thenKnownFields() throws IOException {
        DeviceSnapshot current = snapshot(DEVICE_ID, DeviceStatus.ACTIVE);

        GenericRecord read = new GenericDatumReader<GenericRecord>(DeviceSnapshot.getClassSchema(), SNAPSHOT_1_1)
                .read(null, decoder(writeSpecific(current, DeviceSnapshot.getClassSchema())));

        assertThat(read.get("deviceId").toString()).isEqualTo(DEVICE_ID);
        assertThat(read.get("firmwareVersion").toString()).isEqualTo("1.4.2");
        assertThat(read.getSchema().getField("name")).isNull();
        assertThat(read.getSchema().getField("osName")).isNull();
        assertThat(read.getSchema().getField("osVersion")).isNull();
    }

    private static Schema withoutFields(Schema schema, Set<String> names) {
        List<Schema.Field> fields = schema.getFields().stream()
                .filter(f -> !names.contains(f.name()))
                .map(f -> new Schema.Field(f, f.schema()))
                .toList();
        return Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false, fields);
    }



    private static byte[] writeGeneric(GenericRecord record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static <T> byte[] writeSpecific(T record, Schema schema) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<T>(schema).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static BinaryDecoder decoder(byte[] bytes) {
        return DecoderFactory.get().binaryDecoder(bytes, null);
    }
}