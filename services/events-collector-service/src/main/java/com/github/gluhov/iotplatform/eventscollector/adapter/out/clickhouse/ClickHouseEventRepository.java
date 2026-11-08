package com.github.gluhov.iotplatform.eventscollector.adapter.out.clickhouse;

import com.github.gluhov.iotplatform.eventscollector.domain.model.DeviceEventRecord;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.DeviceEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.Types;

@Repository
@RequiredArgsConstructor
public class ClickHouseEventRepository implements DeviceEventRepository {
    private static final String INSERT = """
            INSERT INTO device_events
                (device_id, event_id, event_date, timestamp_ms, event_type, ingested_at,
                 device_snapshot, source_system, payload,
                 device_name, os_name, os_version, firmware_version)
            VALUES (:deviceId, :eventId, toDate(:eventDate), :timestampMs, :eventType,
                    fromUnixTimestamp64Milli(:ingestedAtMs, 'UTC'),
                    :deviceSnapshot, :sourceSystem, :payload,
                    :deviceName, :osName, :osVersion, :firmwareVersion)
            """;

    private final JdbcClient jdbc;

    @Override
    public void insert(DeviceEventRecord record) {
        jdbc.sql(INSERT)
                .param("deviceId", record.deviceId())
                .param("eventId", record.eventId())
                .param("eventDate", record.eventDate().toString())
                .param("timestampMs", record.timestampMs())
                .param("eventType", record.eventType())
                .param("ingestedAtMs", record.ingestedAt().toEpochMilli())
                .param("deviceSnapshot", record.deviceSnapshot())
                .param("sourceSystem", record.sourceSystem())
                // Nullable(String): тип нужен драйверу, чтобы передать NULL
                .param("payload", record.payload(), Types.VARCHAR)
                .param("deviceName", record.deviceName())
                .param("osName", record.osName())
                .param("osVersion", record.osVersion())
                .param("firmwareVersion", record.firmwareVersion())
                .update();
    }
}