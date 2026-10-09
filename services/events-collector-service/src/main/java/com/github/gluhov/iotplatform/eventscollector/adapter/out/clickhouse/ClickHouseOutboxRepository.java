package com.github.gluhov.iotplatform.eventscollector.adapter.out.clickhouse;

import com.github.gluhov.iotplatform.eventscollector.domain.model.OutboxRecord;
import com.github.gluhov.iotplatform.eventscollector.domain.model.OutboxStatus;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.OutboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class ClickHouseOutboxRepository implements OutboxRepository {
    // Журнал версий: только INSERT, текущее состояние читается через FINAL.
    // Время - в миллисекундах Unix, чтобы не зависеть от часового пояса драйвера и JVM.
    private static final String INSERT = """
            INSERT INTO device_outbox
                (event_id, device_id, source_timestamp_ms, created_at, updated_at,
                state_version, status, attempts, last_error)
            VALUES (:eventId, :deviceId, :sourceTimestampMs,
                    fromUnixTimestamp64Milli(:createdAtMs, 'UTC'),
                    fromUnixTimestamp64Milli(:updatedAtMs, 'UTC'),
                    :stateVersion, :status, :attempts, :lastError)
            """;

    private static final String SELECT_PENDING = """
            SELECT
                event_id,
                device_id,
                source_timestamp_ms,
                toUnixTimestamp64Milli(created_at) AS created_at_ms,
                toUnixTimestamp64Milli(updated_at) AS updated_at_ms,
                state_version,
                toString(status) AS status,
                attempts,
                last_error
            FROM device_outbox FINAL
            WHERE status IN ('NEW', 'FAILED')
                AND attempts < :maxAttempts
            ORDER BY created_at
            LIMIT :limit
            """;

    private static final String COUNT_PENDING = """
            SELECT count()
            FROM device_outbox FINAL
            WHERE status IN ('NEW', 'FAILED')
                AND attempts < :maxAttempts
            """;

    private final JdbcClient jdbcClient;

    @Override
    public void insert(OutboxRecord record) {
        jdbcClient.sql(INSERT)
                .param("eventId", record.eventId())
                .param("deviceId", record.deviceId())
                .param("sourceTimestampMs", record.sourceTimestampMs())
                .param("createdAtMs", record.createdAt().toEpochMilli())
                .param("updatedAtMs", record.updatedAt().toEpochMilli())
                .param("stateVersion", record.stateVersion())
                .param("status", record.status().name())
                .param("attempts", record.attempts())
                .param("lastError", record.lastError())
                .update();
    }

    @Override
    public List<OutboxRecord> findPending(int maxAttempts, int limit) {
        return jdbcClient.sql(SELECT_PENDING)
                .param("maxAttempts", maxAttempts)
                .param("limit", limit)
                .query(ClickHouseOutboxRepository::mapRow)
                .list();
    }

    @Override
    public long countPending(int maxAttempts) {
        return jdbcClient.sql(COUNT_PENDING)
                .param("maxAttempts", maxAttempts)
                .query(Long.class)
                .single();
    }

    private static OutboxRecord mapRow(ResultSet resultSet, int rowNum) throws SQLException {
        return new OutboxRecord(
                resultSet.getString("event_id"),
                resultSet.getString("device_id"),
                resultSet.getLong("source_timestamp_ms"),
                Instant.ofEpochMilli(resultSet.getLong("created_at_ms")),
                Instant.ofEpochMilli(resultSet.getLong("updated_at_ms")),
                resultSet.getLong("state_version"),
                OutboxStatus.valueOf(resultSet.getString("status")),
                resultSet.getInt("attempts"),
                resultSet.getString("last_error")
        );
    }
}
