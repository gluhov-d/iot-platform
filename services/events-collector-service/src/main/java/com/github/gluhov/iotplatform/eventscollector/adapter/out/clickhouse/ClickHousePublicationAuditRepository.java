package com.github.gluhov.iotplatform.eventscollector.adapter.out.clickhouse;

import com.github.gluhov.iotplatform.eventscollector.domain.model.PublicationAuditRecord;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.PublicationAuditRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class ClickHousePublicationAuditRepository implements PublicationAuditRepository {
    private static final String INSERT = """
            INSERT INTO device_publication_audit
                (event_id, device_id, outcome, attempt, error, instance_id, occurred_at)
            VALUES (:eventId, :deviceId, :outcome, :attempt, :error, :instanceId,
                    fromUnixTimestamp64Milli(:occurredAtMs, 'UTC'))
            """;

    private final JdbcClient jdbcClient;
    private final String instanceId;

    // Конструктор написан вручную: Lombok не переносит @Value на параметр конструктора
    public ClickHousePublicationAuditRepository(JdbcClient jdbcClient, @Value("${app.instance-id}") String instanceId) {
        this.jdbcClient = jdbcClient;
        this.instanceId = instanceId;
    }

    @Override
    public void insert(PublicationAuditRecord record) {
        jdbcClient.sql(INSERT)
                .param("eventId", record.eventId())
                .param("deviceId", record.deviceId())
                .param("outcome", record.outcome().name())
                .param("attempt", record.attempt())
                .param("error", record.error())
                .param("instanceId", instanceId)
                .param("occurredAtMs", record.occurredAt().toEpochMilli())
                .update();
    }
}