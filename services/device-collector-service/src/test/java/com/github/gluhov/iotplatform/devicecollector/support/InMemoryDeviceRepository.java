package com.github.gluhov.iotplatform.devicecollector.support;

import com.github.gluhov.iotplatform.devicecollector.domain.exception.StorageAccessException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceRecord;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceState;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceView;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessedEvent;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StorageFailureReason;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StoredDevice;
import com.github.gluhov.iotplatform.devicecollector.domain.port.out.DeviceRepository;
import com.github.gluhov.iotplatform.devicecollector.domain.port.out.TransactionRunner;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Реестр в памяти с той же семантикой, что у SQL в JdbcDeviceRepository (seen_count, LEAST(first_seen_at),
 * GREATEST(last_event_at) и last_event_* только от более позднего события, last_state_at, ON CONFLICT DO NOTHING
 * в журнале), и транзакция с откатом: сценарии доменного сервиса без базы.
 * Шард - тот же расчёт, что в database-devices.yaml: |hashCode| % 2.
 */
public final class InMemoryDeviceRepository implements DeviceRepository, TransactionRunner {
    private final Map<String, Row> devices = new HashMap<>();
    private final Map<String, ProcessedEvent> journal = new HashMap<>();
    private StorageAccessException nextFailure;
    private int queriesBeforeFailure;

    public static String shardOf(String deviceId) {
        return "shard_" + Math.abs(deviceId.hashCode() % 2);
    }

    /** Следующий запрос к хранилищу завершится этой ошибкой (транзакция откатится). */
    public void failNextQuery(StorageFailureReason reason) {
        failQuery(1, reason);
    }

    /** n-й запрос, считая от текущего, завершится ошибкой: сбой посреди транзакции, после уже выполненных запросов. */
    public void failQuery(int nth, StorageFailureReason reason) {
        nextFailure = new StorageAccessException(reason, "simulated " + reason.label(), null);
        queriesBeforeFailure = nth - 1;
    }

    public Row device(String deviceId) {
        return devices.get(deviceId);
    }

    public List<ProcessedEvent> journal(String deviceId) {
        return journal.values().stream().filter(e -> e.deviceId().equals(deviceId)).toList();
    }

    @Override
    public <T> T inTransaction(Supplier<T> work) {
        Map<String, Row> devicesBefore = new HashMap<>();
        devices.forEach((id, row) -> devicesBefore.put(id, row.copy()));
        Map<String, ProcessedEvent> journalBefore = new HashMap<>(journal);
        try {
            return work.get();
        } catch (RuntimeException e) {
            devices.clear();
            devices.putAll(devicesBefore);
            journal.clear();
            journal.putAll(journalBefore);
            throw e;
        }
    }

    @Override
    public Optional<StoredDevice> lockById(String deviceId) {
        failIfRequested();
        Row row = devices.get(deviceId);
        return row == null ? Optional.empty()
                : Optional.of(new StoredDevice(shardOf(deviceId), row.status, row.lastEventAt, row.lastStateAt,
                row.lastSeenAt, row.deletedAt));
    }

    @Override
    public boolean markProcessed(ProcessedEvent event) {
        failIfRequested();
        return journal.putIfAbsent(event.deviceId() + "|" + event.eventKey(), event) == null;
    }

    @Override
    public String insert(DeviceRecord record) {
        failIfRequested();
        if (devices.containsKey(record.deviceId())) {
            throw new StorageAccessException(StorageFailureReason.OTHER, "duplicate key devices_pkey", null);
        }
        Row row = new Row();
        row.seenCount = 1;
        row.firstSeenAt = record.eventAt();
        row.lastEventAt = record.eventAt();
        row.lastEventId = record.eventId();
        row.lastEventType = record.eventType().name();
        row.state = record.state();
        row.status = record.status();
        row.lastSeenAt = record.lastSeenAt();
        row.deletedAt = record.deletedAt();
        row.lastStateAt = record.stateAt();
        devices.put(record.deviceId(), row);
        return shardOf(record.deviceId());
    }

    @Override
    public String updateWithState(DeviceRecord record) {
        failIfRequested();
        Row row = existing(record.deviceId());
        row.state = record.state();
        row.status = record.status();
        row.lastSeenAt = record.lastSeenAt();
        row.deletedAt = record.deletedAt();
        row.lastStateAt = record.stateAt();
        countEvent(row, record);
        return shardOf(record.deviceId());
    }

    @Override
    public String updateWithoutState(DeviceRecord record) {
        failIfRequested();
        Row row = existing(record.deviceId());
        row.status = record.status();
        row.lastSeenAt = record.lastSeenAt();
        row.deletedAt = record.deletedAt();
        if (record.stateAt() != null) {
            row.lastStateAt = record.stateAt();
        }
        countEvent(row, record);
        return shardOf(record.deviceId());
    }

    @Override
    public String countStale(String deviceId, Instant eventAt) {
        failIfRequested();
        Row row = existing(deviceId);
        row.seenCount++;
        if (eventAt.isBefore(row.firstSeenAt)) {
            row.firstSeenAt = eventAt;
        }
        return shardOf(deviceId);
    }

    @Override
    public Optional<DeviceView> findById(String deviceId) {
        failIfRequested();
        Row row = devices.get(deviceId);
        if (row == null) {
            return Optional.empty();
        }
        // Как toView в JdbcDeviceRepository: статус и last_seen_at - из колонок, их меняют и события без снимка
        DeviceState s = row.state;
        DeviceState state = s == null
                ? new DeviceState(deviceId, null, null, null, null, null, null, null, null, null, null, row.status,
                null, row.lastSeenAt, Map.of(), Map.of(), List.of())
                : new DeviceState(s.deviceId(), s.tenantId(), s.ownerId(), s.deviceType(), s.serialNumber(),
                s.manufacturer(), s.model(), s.firmwareVersion(), s.name(), s.osName(), s.osVersion(), row.status,
                s.registeredAt(), row.lastSeenAt, s.settings(), s.attributes(), s.sensors());
        return Optional.of(new DeviceView(state, row.seenCount, row.firstSeenAt, row.lastEventAt, row.lastEventId,
                row.lastEventType, row.deletedAt, shardOf(deviceId)));
    }

    // COUNT_EVENT в JdbcDeviceRepository: last_event_* - только от более позднего события
    private static void countEvent(Row row, DeviceRecord record) {
        row.seenCount++;
        if (record.eventAt().isBefore(row.firstSeenAt)) {
            row.firstSeenAt = record.eventAt();
        }
        if (!record.eventAt().isBefore(row.lastEventAt)) {
            row.lastEventAt = record.eventAt();
            row.lastEventId = record.eventId();
            row.lastEventType = record.eventType().name();
        }
    }

    // UPDATE ... RETURNING по отсутствующей строке: .single() адаптера даёт ошибку хранилища, а не NPE
    private Row existing(String deviceId) {
        Row row = devices.get(deviceId);
        if (row == null) {
            throw new StorageAccessException(StorageFailureReason.OTHER, "no row for " + deviceId, null);
        }
        return row;
    }

    private void failIfRequested() {
        if (nextFailure != null && queriesBeforeFailure-- > 0) {
            return;
        }
        if (nextFailure != null) {
            StorageAccessException failure = nextFailure;
            nextFailure = null;
            throw failure;
        }
    }

    /** Строка devices. */
    public static final class Row {
        public DeviceState state;
        public String status;
        public long seenCount;
        public Instant firstSeenAt;
        public Instant lastEventAt;
        public Instant lastSeenAt;
        public Instant deletedAt;
        public String lastEventId;
        public String lastEventType;
        public Instant lastStateAt;

        Row copy() {
            Row copy = new Row();
            copy.state = state;
            copy.status = status;
            copy.seenCount = seenCount;
            copy.firstSeenAt = firstSeenAt;
            copy.lastEventAt = lastEventAt;
            copy.lastSeenAt = lastSeenAt;
            copy.deletedAt = deletedAt;
            copy.lastEventId = lastEventId;
            copy.lastEventType = lastEventType;
            copy.lastStateAt = lastStateAt;
            return copy;
        }
    }
}