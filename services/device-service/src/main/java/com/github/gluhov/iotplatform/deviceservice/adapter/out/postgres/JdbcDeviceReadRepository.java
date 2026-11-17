package com.github.gluhov.iotplatform.deviceservice.adapter.out.postgres;

import com.github.gluhov.iotplatform.deviceservice.domain.exception.ReadModelUnavailableException;
import com.github.gluhov.iotplatform.deviceservice.domain.model.Device;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceSensor;
import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceReadRepository;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Чтение карточки из логической базы devices_read (ShardingSphere-Proxy, реплики шардов). Запрос содержит device_id -
 * Proxy отправляет его на один шард. Карточка есть, если устройство не удалено и у него есть tenant_id (data-model.md).
 * Тайм-ауты соединения задаются в URL базы (connectTimeout, socketTimeout) - из окружения.
 */
@Repository
public class JdbcDeviceReadRepository implements DeviceReadRepository {

    private static final String FIND = """
            SELECT device_id, name, tenant_id, owner_id, device_type, serial_number, manufacturer, model, status,
                   settings, attributes, firmware_version, os_name, os_version, sensors, registered_at, last_seen_at,
                   version
            FROM devices
            WHERE device_id = :deviceId AND deleted_at IS NULL AND tenant_id IS NOT NULL""";

    // Статусы контракта DeviceStatus; прочее (значения, добавленные позже) отдаётся как UNKNOWN
    private static final Set<String> CONTRACT_STATUSES =
            Set.of("UNKNOWN", "NEW", "ACTIVE", "INACTIVE", "MAINTENANCE", "BLOCKED");
    private static final TypeReference<Map<String, String>> STRING_MAP = new TypeReference<>() {};
    private static final TypeReference<List<SensorJson>> SENSORS = new TypeReference<>() {};
    // JSONB читается тем же форматом, что пишет device-collector-service (measuredAtMs - миллисекунды Unix)
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final JdbcClient jdbc;

    public JdbcDeviceReadRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Optional<Device> findById(String deviceId) {
        try {
            return jdbc.sql(FIND)
                    .param("deviceId", deviceId, Types.VARCHAR)
                    .query((rs, row) -> toDevice(rs))
                    .optional();
        } catch (DataAccessException | JacksonException e) {
            throw new ReadModelUnavailableException("Чтение устройства из devices_read", e);
        }
    }

    private static Device toDevice(ResultSet rs) throws SQLException {
        return new Device(rs.getString("device_id"), rs.getString("name"), rs.getString("tenant_id"),
                rs.getString("owner_id"), rs.getString("device_type"), rs.getString("serial_number"),
                rs.getString("manufacturer"), rs.getString("model"), normalizeStatus(rs.getString("status")),
                JSON.readValue(rs.getString("settings"), STRING_MAP),
                JSON.readValue(rs.getString("attributes"), STRING_MAP),
                rs.getString("firmware_version"), rs.getString("os_name"), rs.getString("os_version"),
                parseSensors(rs.getString("sensors")),
                instant(rs, "registered_at"), instant(rs, "last_seen_at"), rs.getLong("version"));
    }

    static String normalizeStatus(String status) {
        return status != null && CONTRACT_STATUSES.contains(status) ? status : "UNKNOWN";
    }

    static List<DeviceSensor> parseSensors(String json) {
        return JSON.readValue(json, SENSORS).stream().map(SensorJson::toSensor).toList();
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    /** Сенсор в JSONB: поля как в device-collector-service. */
    record SensorJson(String sensorId, String sensorType, String unit, String value, Long measuredAtMs, String status) {

        DeviceSensor toSensor() {
            return new DeviceSensor(sensorId, sensorType, unit, value,
                    measuredAtMs == null ? null : Instant.ofEpochMilli(measuredAtMs), status);
        }
    }
}
