package com.github.gluhov.iotplatform.devicecollector.service.contract;

import com.github.gluhov.iotplatform.devicecollector.domain.model.ChangeType;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceChange;
import com.github.gluhov.iotplatform.devicecollector.domain.service.DeviceInfoMapper;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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

import static com.github.gluhov.iotplatform.devicecollector.data.TestData.POSITION;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.T0;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.T0_MS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Совместимость с модулем 2: в теме devices могут лежать сообщения DeviceInfo, записанные схемой
 * contracts-avro 1.0.0 (тег contracts/v1.0.0). Сервис читает их схемой 1.1.0: новые поля получают значения по умолчанию.
 */
class ContractCompatibilityTest {
    // contracts/avro/device-info.avsc из тега contracts/v1.0.0
    private static final Schema DEVICE_INFO_1_0_0 = new Schema.Parser().parse("""
            {"type": "record", "name": "DeviceInfo", "namespace": "com.github.gluhov.iotplatform.events.avro",
             "fields": [{"name": "deviceId", "type": "string"}, {"name": "emittedAtMs", "type": "long"}]}""");

    @Test
    @DisplayName("DeviceInfo written with contracts 1.0.0 is read by 1.1.0 with defaults and processed as SNAPSHOT")
    void givenMessageOfVersion100_whenReadWith110_thenDefaultsAndLegacyKey() throws IOException {
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