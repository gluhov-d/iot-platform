package com.github.gluhov.iotplatform.failedeventsprocessor.contract;

import com.github.gluhov.iotplatform.events.avro.ErrorCode;
import com.github.gluhov.iotplatform.events.avro.ErrorType;
import com.github.gluhov.iotplatform.events.avro.FailedEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Контракт FailedEvent 1.4.0 (FR-001, R9): запись, написанная схемой 1.4.0, читается процессором;
 * символ enum, которого процессор не знает, даёт UNKNOWN по default схемы читателя.
 */
class FailedEventContractTest {

    @Test
    void givenRecordWrittenWithSchema140_whenRead_thenFieldsEqual() throws IOException {
        FailedEvent written = sample(ErrorCode.VALIDATION_FAILED);

        FailedEvent read = read(write(FailedEvent.getClassSchema(), written), FailedEvent.getClassSchema());

        assertThat(read.getSourceService()).isEqualTo("events-collector-service");
        assertThat(read.getSourceOffset()).isEqualTo(1042L);
        assertThat(read.getErrorCode()).isEqualTo(ErrorCode.VALIDATION_FAILED);
        assertThat(read.getErrorType()).isEqualTo(ErrorType.PERMANENT);
        assertThat(read.getTraceId()).isEqualTo("0af7651916cd43dd8448eb211c80319c");
        assertThat(read.getDeviceId()).isNull();
    }

    @Test
    void givenUnknownErrorCodeSymbolFromNewerWriter_whenRead_thenErrorCodeIsUnknown() throws IOException {
        Schema newerWriter = withExtraErrorCodeSymbol("FUTURE_CODE");
        GenericRecord record = new GenericData.Record(newerWriter);
        record.put("sourceService", "events-collector-service");
        record.put("sourceTopic", "events");
        record.put("sourcePartition", 2);
        record.put("sourceOffset", 1042L);
        record.put("sourceTimestamp", Instant.parse("2026-11-21T10:15:30.123Z").toEpochMilli());
        record.put("failedAt", Instant.parse("2026-11-21T10:15:31.480Z").toEpochMilli());
        record.put("errorCode", new GenericData.EnumSymbol(newerWriter.getField("errorCode").schema(), "FUTURE_CODE"));
        record.put("errorType", new GenericData.EnumSymbol(newerWriter.getField("errorType").schema(), "PERMANENT"));
        record.put("description", "InvalidEventException: value must be finite");
        record.put("traceId", null);
        record.put("key", null);
        record.put("value", ByteBuffer.wrap(new byte[]{1, 2, 3}));
        record.put("headers", Map.of());
        record.put("attempts", 1);
        record.put("deviceId", null);

        FailedEvent read = read(write(newerWriter, record), newerWriter);

        assertThat(read.getErrorCode()).isEqualTo(ErrorCode.UNKNOWN);
        assertThat(read.getSourceOffset()).isEqualTo(1042L);
        assertThat(read.getSourcePartition()).isEqualTo(2);
    }

    private static FailedEvent sample(ErrorCode code) {
        return FailedEvent.newBuilder()
                .setSourceService("events-collector-service")
                .setSourceTopic("events")
                .setSourcePartition(2)
                .setSourceOffset(1042L)
                .setSourceTimestamp(Instant.parse("2026-11-21T10:15:30.123Z"))
                .setFailedAt(Instant.parse("2026-11-21T10:15:31.480Z"))
                .setErrorCode(code)
                .setErrorType(ErrorType.PERMANENT)
                .setDescription("InvalidEventException: value must be finite")
                .setTraceId("0af7651916cd43dd8448eb211c80319c")
                .setKey(null)
                .setValue(ByteBuffer.wrap("payload".getBytes(StandardCharsets.UTF_8)))
                .setHeaders(Map.of())
                .setAttempts(1)
                .setDeviceId(null)
                .build();
    }

    /** Схема писателя: та же 1.4.0, но у ErrorCode есть символ, которого не знает читатель. */
    private static Schema withExtraErrorCodeSymbol(String symbol) {
        String original = FailedEvent.getClassSchema().toString();
        String changed = original.replace("\"PROCESSING_FAILED\"]", "\"PROCESSING_FAILED\",\"" + symbol + "\"]");
        assertThat(changed).as("в схеме ErrorCode должен добавиться символ").isNotEqualTo(original);
        return new Schema.Parser().parse(changed);
    }

    private static byte[] write(Schema writerSchema, Object value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<Object> writer = value instanceof FailedEvent
                ? new SpecificDatumWriter<>(writerSchema)
                : new GenericDatumWriter<>(writerSchema);
        writer.write(value, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    /** Читатель - FailedEvent 1.4.0 (specific), писатель - схема, которой записано сообщение. */
    private static FailedEvent read(byte[] bytes, Schema writerSchema) throws IOException {
        return new SpecificDatumReader<FailedEvent>(writerSchema, FailedEvent.getClassSchema())
                .read(null, DecoderFactory.get().binaryDecoder(bytes, null));
    }
}
