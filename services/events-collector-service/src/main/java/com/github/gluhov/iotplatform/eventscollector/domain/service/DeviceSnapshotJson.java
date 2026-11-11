package com.github.gluhov.iotplatform.eventscollector.domain.service;

import com.github.gluhov.iotplatform.events.avro.DeviceSnapshot;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Снимок устройства в ClickHouse (device_events, device_outbox) хранится как Avro JSON: строку можно прочитать
 * в clickhouse-client, а публикатор восстанавливает из неё тот же DeviceSnapshot без потерь.
 * <p>
 * Avro JSON читается только схемой, которой записан: в нём нет значений по умолчанию для полей, которых не было
 * у писателя. Поэтому в device_outbox рядом со снимком хранится схема писателя (device_snapshot_schema), и запись
 * старой версии контрактов читается разрешением схем: писатель - сохранённая схема, читатель - текущая
 * (новые поля получают default). Пустая схема - текущая.
 */
public final class DeviceSnapshotJson {
    private static final String CURRENT = DeviceSnapshot.getClassSchema().toString();
    private static final Map<String, Schema> WRITERS = new ConcurrentHashMap<>();

    private DeviceSnapshotJson() {
    }

    /** Схема, которой toJson записывает снимок: сохраняется вместе со снимком. */
    public static String writerSchema() {
        return CURRENT;
    }

    /** Пустая строка, если снимка нет. */
    public static String toJson(DeviceSnapshot snapshot) {
        if (snapshot == null) {
            return "";
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(DeviceSnapshot.getClassSchema(), out);
            new SpecificDatumWriter<>(DeviceSnapshot.class).write(snapshot, encoder);
            encoder.flush();
            return out.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("DeviceSnapshot is not serializable to Avro JSON", e);
        }
    }

    /** Снимок, записанный текущей схемой; null, если снимка нет (пустая строка). */
    public static DeviceSnapshot fromJson(String json) {
        return fromJson(json, "");
    }

    /**
     * Снимок, записанный схемой writerSchema (пустая - текущая); null, если снимка нет.
     *
     * @throws IllegalArgumentException строка не читается этой схемой - дефект данных, повтор не поможет
     */
    public static DeviceSnapshot fromJson(String json, String writerSchema) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        Schema writer = writerSchema == null || writerSchema.isEmpty() || CURRENT.equals(writerSchema)
                ? DeviceSnapshot.getClassSchema()
                : WRITERS.computeIfAbsent(writerSchema, schema -> new Schema.Parser().parse(schema));
        try {
            return new SpecificDatumReader<DeviceSnapshot>(writer, DeviceSnapshot.getClassSchema())
                    .read(null, DecoderFactory.get().jsonDecoder(writer, json));
        } catch (IOException | AvroRuntimeException e) {
            throw new IllegalArgumentException("Stored device snapshot is not valid Avro JSON for its schema", e);
        }
    }
}