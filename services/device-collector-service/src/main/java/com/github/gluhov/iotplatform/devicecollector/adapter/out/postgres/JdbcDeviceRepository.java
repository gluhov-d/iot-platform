package com.github.gluhov.iotplatform.devicecollector.adapter.out.postgres;

import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceRecord;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceState;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceView;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessedEvent;
import com.github.gluhov.iotplatform.devicecollector.domain.model.SensorState;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StoredDevice;
import com.github.gluhov.iotplatform.devicecollector.domain.port.out.DeviceRepository;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Реестр через ShardingSphere-Proxy. Каждый запрос содержит device_id - Proxy отправляет его на один шард.
 * Значения - только параметрами prepared statement: литерал с экранированной кавычкой ('o''neil') Proxy 5.5.3
 * маршрутизирует по неразобранному тексту, то есть на другой шард.
 * JSONB - строкой без приведения типа: Proxy теряет ?::jsonb в INSERT, тип выводит шард (stringtype=unspecified
 * в database-devices.yaml). Шард в ответе - current_database() того узла, который выполнил запрос.
 */
@Repository
public class JdbcDeviceRepository implements DeviceRepository {
    // Учёт события в строке: снимок, давший состояние, может быть старше уже известного события устройства
    // (снимок регистрации пришёл после телеметрии) - тогда last_event_* остаются от более позднего события.
    // В SET все выражения видят значения строки до UPDATE
    private static final String COUNT_EVENT = """
                   seen_count = seen_count + 1, first_seen_at = LEAST(first_seen_at, :eventAt),
                   last_event_id = CASE WHEN :eventAt >= last_event_at THEN :eventId ELSE last_event_id END,
                   last_event_type = CASE WHEN :eventAt >= last_event_at THEN :eventType ELSE last_event_type END,
                   last_event_at = GREATEST(last_event_at, :eventAt), updated_at = now()
            """;
    private static final String LOCK = """
            SELECT current_database() AS shard, status, last_event_at, last_state_at, last_seen_at, deleted_at
            FROM devices WHERE device_id = :deviceId FOR UPDATE""";
    private static final String MARK_PROCESSED = """
            INSERT INTO processed_device_events (device_id, event_key, event_type, event_at, emitted_at, outcome,
                                                 kafka_topic, kafka_partition, kafka_offset)
            VALUES (:deviceId, :eventKey, :eventType, :eventAt, :emittedAt, :outcome, :topic, :partition, :offset)
            ON CONFLICT (device_id, event_key) DO NOTHING""";
    private static final String INSERT = """
            INSERT INTO devices (device_id, tenant_id, owner_id, device_type, serial_number, manufacturer, model,
                                 firmware_version, name, os_name, os_version, status, registered_at, last_seen_at,
                                 settings, attributes, sensors, seen_count, first_seen_at, last_event_at,
                                 last_event_id, last_event_type, last_state_at, deleted_at)
            VALUES (:deviceId, :tenantId, :ownerId, :deviceType, :serialNumber, :manufacturer, :model,
                    :firmwareVersion, :name, :osName, :osVersion, :status, :registeredAt, :lastSeenAt,
                    :settings, :attributes, :sensors, 1, :eventAt, :eventAt, :eventId, :eventType, :stateAt,
                    :deletedAt)
            RETURNING current_database()""";
    private static final String UPDATE_WITH_STATE = """
            UPDATE devices SET tenant_id = :tenantId, owner_id = :ownerId, device_type = :deviceType,
                   serial_number = :serialNumber, manufacturer = :manufacturer, model = :model,
                   firmware_version = :firmwareVersion, name = :name, os_name = :osName, os_version = :osVersion,
                   status = :status, registered_at = :registeredAt,
                   last_seen_at = :lastSeenAt, settings = :settings, attributes = :attributes, sensors = :sensors,
                   last_state_at = :stateAt, deleted_at = :deletedAt,
            """ + COUNT_EVENT + """
            WHERE device_id = :deviceId
            RETURNING current_database()""";
    private static final String UPDATE_WITHOUT_STATE = """
            UPDATE devices SET status = :status, last_seen_at = :lastSeenAt,
                   last_state_at = COALESCE(:stateAt, last_state_at), deleted_at = :deletedAt,
            """ + COUNT_EVENT + """
            WHERE device_id = :deviceId
            RETURNING current_database()""";
    private static final String COUNT_STALE = """
            UPDATE devices SET seen_count = seen_count + 1, first_seen_at = LEAST(first_seen_at, :eventAt),
                   updated_at = now()
            WHERE device_id = :deviceId
            RETURNING current_database()""";
    private static final String FIND = """
            SELECT current_database() AS shard, device_id, tenant_id, owner_id, device_type, serial_number,
                   manufacturer, model, firmware_version, name, os_name, os_version, status, registered_at,
                   last_seen_at, settings, attributes, sensors, seen_count, first_seen_at, last_event_at,
                   last_event_id, last_event_type, deleted_at
            FROM devices WHERE device_id = :deviceId""";

    private static final TypeReference<Map<String, String>> STRING_MAP = new TypeReference<>() {};
    private static final TypeReference<List<SensorJson>> SENSORS = new TypeReference<>() {};

    private final JdbcClient jdbc;
    // Собственный формат хранения JSONB, не зависит от настроек JSON веб-слоя
    private final JsonMapper json = JsonMapper.builder().build();

    public JdbcDeviceRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public Optional<StoredDevice> lockById(String deviceId) {
        return run("Select device for update", () -> jdbc.sql(LOCK)
                .param("deviceId", deviceId, Types.VARCHAR)
                .query((rs, row) -> new StoredDevice(rs.getString("shard"), rs.getString("status"),
                        instant(rs, "last_event_at"), instant(rs, "last_state_at"), instant(rs, "last_seen_at"),
                        instant(rs, "deleted_at")))
                .optional());
    }

    @Override
    public boolean markProcessed(ProcessedEvent event) {
        return run("Insert processed_device_events", () -> jdbc.sql(MARK_PROCESSED)
                .param("deviceId", event.deviceId(), Types.VARCHAR)
                .param("eventKey", event.eventKey(), Types.VARCHAR)
                .param("eventType", event.eventType().name(), Types.VARCHAR)
                .param("eventAt", timestamp(event.eventAt()), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("emittedAt", timestamp(event.emittedAt()), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("outcome", event.outcome().name(), Types.VARCHAR)
                .param("topic", event.position().topic(), Types.VARCHAR)
                .param("partition", event.position().partition(), Types.INTEGER)
                .param("offset", event.position().offset(), Types.BIGINT)
                .update() == 1);
    }

    @Override
    public String insert(DeviceRecord record) {
        return run("Insert devices", () -> withState(jdbc.sql(INSERT), record).query(String.class).single());
    }

    @Override
    public String updateWithState(DeviceRecord record) {
        return run("Update devices", () -> withState(jdbc.sql(UPDATE_WITH_STATE), record).query(String.class).single());
    }

    @Override
    public String updateWithoutState(DeviceRecord record) {
        return run("Update devices", () -> withEvent(jdbc.sql(UPDATE_WITHOUT_STATE), record).query(String.class).single());
    }

    @Override
    public String countStale(String deviceId, Instant eventAt) {
        return run("Update devices", () -> jdbc.sql(COUNT_STALE)
                .param("deviceId", deviceId, Types.VARCHAR)
                .param("eventAt", timestamp(eventAt), Types.TIMESTAMP_WITH_TIMEZONE)
                .query(String.class)
                .single());
    }

    @Override
    public Optional<DeviceView> findById(String deviceId) {
        return run("Select device", () -> jdbc.sql(FIND)
                .param("deviceId", deviceId, Types.VARCHAR)
                .query((rs, row) -> toView(rs))
                .optional());
    }

    // Поля модели: у события без снимка их нет, у нового устройства без снимка - NULL и значения по умолчанию
    private JdbcClient.StatementSpec withState(JdbcClient.StatementSpec spec, DeviceRecord record) {
        DeviceState state = record.state();
        return withEvent(spec, record)
                .param("tenantId", state == null ? null : state.tenantId(), Types.VARCHAR)
                .param("ownerId", state == null ? null : state.ownerId(), Types.VARCHAR)
                .param("deviceType", state == null ? null : state.deviceType(), Types.VARCHAR)
                .param("serialNumber", state == null ? null : state.serialNumber(), Types.VARCHAR)
                .param("manufacturer", state == null ? null : state.manufacturer(), Types.VARCHAR)
                .param("model", state == null ? null : state.model(), Types.VARCHAR)
                .param("firmwareVersion", state == null ? null : state.firmwareVersion(), Types.VARCHAR)
                .param("name", state == null ? null : state.name(), Types.VARCHAR)
                .param("osName", state == null ? null : state.osName(), Types.VARCHAR)
                .param("osVersion", state == null ? null : state.osVersion(), Types.VARCHAR)
                .param("registeredAt", state == null ? null : timestamp(state.registeredAt()), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("settings", toJson(state == null ? Map.of() : state.settings()), Types.VARCHAR)
                .param("attributes", toJson(state == null ? Map.of() : state.attributes()), Types.VARCHAR)
                .param("sensors", toJson(state == null ? List.of() : state.sensors().stream().map(SensorJson::of).toList()),
                        Types.VARCHAR);
    }

    private static JdbcClient.StatementSpec withEvent(JdbcClient.StatementSpec spec, DeviceRecord record) {
        return spec.param("deviceId", record.deviceId(), Types.VARCHAR)
                .param("status", record.status(), Types.VARCHAR)
                .param("lastSeenAt", timestamp(record.lastSeenAt()), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("eventAt", timestamp(record.eventAt()), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("eventId", record.eventId(), Types.VARCHAR)
                .param("eventType", record.eventType().name(), Types.VARCHAR)
                .param("deletedAt", timestamp(record.deletedAt()), Types.TIMESTAMP_WITH_TIMEZONE)
                .param("stateAt", timestamp(record.stateAt()), Types.TIMESTAMP_WITH_TIMEZONE);
    }

    private DeviceView toView(ResultSet rs) throws SQLException {
        List<SensorState> sensors = json.readValue(rs.getString("sensors"), SENSORS).stream()
                .map(SensorJson::toState)
                .toList();
        DeviceState state = new DeviceState(rs.getString("device_id"), rs.getString("tenant_id"),
                rs.getString("owner_id"), rs.getString("device_type"), rs.getString("serial_number"),
                rs.getString("manufacturer"), rs.getString("model"), rs.getString("firmware_version"),
                rs.getString("name"), rs.getString("os_name"), rs.getString("os_version"),
                rs.getString("status"), instant(rs, "registered_at"), instant(rs, "last_seen_at"),
                json.readValue(rs.getString("settings"), STRING_MAP),
                json.readValue(rs.getString("attributes"), STRING_MAP), sensors);
        return new DeviceView(state, rs.getLong("seen_count"), instant(rs, "first_seen_at"), instant(rs, "last_event_at"),
                rs.getString("last_event_id"), rs.getString("last_event_type"), instant(rs, "deleted_at"),
                rs.getString("shard"));
    }

    private String toJson(Object value) {
        return json.writeValueAsString(value);
    }

    private static <T> T run(String operation, Supplier<T> query) {
        try {
            return query.get();
        } catch (DataAccessException e) {
            throw SqlErrors.toStorageException(operation, e);
        }
    }

    private static OffsetDateTime timestamp(Instant instant) {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    /** Сенсор в JSONB: поля и единицы как в контракте (measuredAtMs - миллисекунды Unix). */
    record SensorJson(String sensorId, String sensorType, String unit, String value, Long measuredAtMs, String status) {

        static SensorJson of(SensorState sensor) {
            return new SensorJson(sensor.sensorId(), sensor.sensorType(), sensor.unit(), sensor.value(),
                    sensor.measuredAt() == null ? null : sensor.measuredAt().toEpochMilli(), sensor.status());
        }

        SensorState toState() {
            return new SensorState(sensorId, sensorType, unit, value,
                    measuredAtMs == null ? null : Instant.ofEpochMilli(measuredAtMs), status);
        }
    }
}