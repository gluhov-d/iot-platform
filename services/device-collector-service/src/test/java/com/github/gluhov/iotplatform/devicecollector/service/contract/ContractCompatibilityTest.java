package com.github.gluhov.iotplatform.devicecollector.service.contract;

import com.github.gluhov.iotplatform.devicecollector.domain.model.ChangeType;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceChange;
import com.github.gluhov.iotplatform.devicecollector.domain.service.DeviceInfoMapper;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import com.github.gluhov.iotplatform.events.avro.OperationType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import static com.github.gluhov.iotplatform.devicecollector.data.TestData.POSITION;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.T0;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.T0_MS;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.command;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.info;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Совместимость с модулем 2: в теме devices могут лежать сообщения DeviceInfo, записанные схемой
 * contracts-avro 1.0.0 (тег contracts/v1.0.0). Сервис читает их текущей схемой (avros 1.3.0): новые поля получают значения по умолчанию.
 */
class ContractCompatibilityTest {
    private static final String DEVICE_ID = "dev-1";
    private static final String EVENT_ID = "evt-1";
    // contracts/avro/device-info.avsc из тега contracts/v1.0.0
    private static final Schema DEVICE_INFO_1_0_0 = new Schema.Parser().parse("""
            {"type": "record", "name": "DeviceInfo", "namespace": "com.github.gluhov.iotplatform.events.avro",
             "fields": [{"name": "deviceId", "type": "string"}, {"name": "emittedAtMs", "type": "long"}]}""");

    @Test
    @DisplayName("DeviceInfo written with contracts 1.0.0 is read by the current schema (1.3.0) with defaults and processed as SNAPSHOT")
    void givenMessageOfVersion100_whenReadWithCurrentSchema_thenDefaultsAndLegacyKey() throws IOException {
        GenericRecord old = new GenericData.Record(DEVICE_INFO_1_0_0);
        old.put("deviceId", "dev-1");
        old.put("emittedAtMs", T0_MS);

        DeviceInfo read = readAsCurrent(write(old));

        assertThat(read.getDeviceId()).isEqualTo("dev-1");
        assertThat(read.getEmittedAtMs()).isEqualTo(T0_MS);
        assertThat(read.getEventType()).isEqualTo(DeviceUpsertEventType.SNAPSHOT);
        assertThat(read.getEventId()).isNull();
        assertThat(read.getEventTimeMs()).isNull();
        assertThat(read.getDevice()).isNull();

        DeviceChange change = new DeviceInfoMapper().toChange(read, POSITION);
        assertThat(change.type()).isEqualTo(ChangeType.SNAPSHOT);
        assertThat(change.eventKey()).isEqualTo("emitted:" + T0_MS);
        assertThat(change.eventAt()).isEqualTo(T0);
    }

    /** DeviceInfo 1.2.0 - текущая схема без operation (поле добавлено в 1.3.0 последним). */
    private static final Schema DEVICE_INFO_1_2 = withoutFields(DeviceInfo.getClassSchema(), Set.of("operation"));

    @Test
    @DisplayName("DeviceInfo 1.2.0 read by 1.3.0 has no command and is a router event for the mapper")
    void givenDeviceInfo12_whenReadWith130_thenNoCommand() throws IOException {
        DeviceInfo current = info(DEVICE_ID, EVENT_ID, DeviceUpsertEventType.UPDATED, T0).build();
        GenericRecord old = new GenericData.Record(DEVICE_INFO_1_2);
        DEVICE_INFO_1_2.getFields().forEach(f -> old.put(f.name(), current.get(f.name())));

        DeviceInfo read = readAsCurrent(DEVICE_INFO_1_2, write(old));

        assertThat(read.getOperation()).isNull();
        DeviceChange change = new DeviceInfoMapper().toChange(read, POSITION);
        assertThat(change.command()).isNull();
        assertThat(change.isCommand()).isFalse();
        assertThat(change.type()).isEqualTo(ChangeType.UPDATED);
        assertThat(change.eventId()).isEqualTo(EVENT_ID);
    }

    @Test
    @DisplayName("DeviceInfo 1.3.0 with operation read by 1.2.0 - operation dropped, the rest unchanged")
    void givenDeviceInfo13WithOperation_whenReadWith120_thenOperationDropped() throws IOException {
        DeviceInfo current = command(DEVICE_ID, EVENT_ID, OperationType.UPDATE, T0).build();
        GenericRecord current13 = new GenericData.Record(DeviceInfo.getClassSchema());
        DeviceInfo.getClassSchema().getFields().forEach(f -> current13.put(f.name(), current.get(f.name())));

        GenericRecord read = new GenericDatumReader<GenericRecord>(DeviceInfo.getClassSchema(), DEVICE_INFO_1_2)
                .read(null, DecoderFactory.get().binaryDecoder(write(current13), null));

        assertThat(read.get("deviceId").toString()).isEqualTo(DEVICE_ID);
        assertThat(read.get("eventId").toString()).isEqualTo(EVENT_ID);
        assertThat(read.getSchema().getField("operation")).isNull();
    }

    private static DeviceInfo readAsCurrent(Schema writer, byte[] bytes) throws IOException {
        SpecificDatumReader<DeviceInfo> reader = new SpecificDatumReader<>(writer, DeviceInfo.getClassSchema());
        return reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
    }

    private static Schema withoutFields(Schema schema, Set<String> names) {
        List<Schema.Field> fields = schema.getFields().stream()
                .filter(f -> !names.contains(f.name()))
                .map(f -> new Schema.Field(f, f.schema()))
                .toList();
        return Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false, fields);
    }

    private static byte[] write(GenericRecord record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(record.getSchema()).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static DeviceInfo readAsCurrent(byte[] bytes) throws IOException {
        SpecificDatumReader<DeviceInfo> reader = new SpecificDatumReader<>(DEVICE_INFO_1_0_0, DeviceInfo.getClassSchema());
        return reader.read(null, DecoderFactory.get().binaryDecoder(bytes, null));
    }
}