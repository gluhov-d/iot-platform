package com.github.gluhov.iotplatform.devicecollector.service.domain.service;

import com.github.gluhov.iotplatform.devicecollector.domain.exception.DeviceNotStoredException;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.InvalidDeviceInfoException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.FailureReason;
import com.github.gluhov.iotplatform.devicecollector.domain.model.MessagePosition;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessingOutcome;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessingResult;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StorageFailureReason;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ValidationPolicy;
import com.github.gluhov.iotplatform.devicecollector.domain.port.out.DeviceCacheInvalidator;
import com.github.gluhov.iotplatform.devicecollector.domain.service.DeviceInfoMapper;
import com.github.gluhov.iotplatform.devicecollector.domain.service.DeviceInfoValidator;
import com.github.gluhov.iotplatform.devicecollector.domain.service.DeviceRegistryService;
import com.github.gluhov.iotplatform.devicecollector.support.InMemoryDeviceRepository;
import com.github.gluhov.iotplatform.devicecollector.support.MetricsFacade;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceStatus;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import com.github.gluhov.iotplatform.events.avro.OperationType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.gluhov.iotplatform.devicecollector.data.TestData.POSITION;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.T0;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.adminSnapshot;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.command;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.info;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.legacy;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.snapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Сценарии обработки на реестре в памяти с семантикой SQL-запросов адаптера. */
class DeviceRegistryServiceTest {
    private static final String DEVICE = "dev-1";
    private static final String SHARD = InMemoryDeviceRepository.shardOf(DEVICE);

    private final InMemoryDeviceRepository store = new InMemoryDeviceRepository();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final List<String> resolvedShards = new ArrayList<>();
    private final MetricsFacade metrics = new MetricsFacade(registry);
    // Сброс кэша: запоминаем устройство@версия; redisDown - Redis недоступен (порт не бросает исключений)
    private final List<String> raisedFloors = new ArrayList<>();
    private boolean redisDown;
    private final DeviceCacheInvalidator cache = (deviceId, version) -> {
        raisedFloors.add(deviceId + "@" + version);
        return !redisDown;
    };
    private final DeviceRegistryService service = new DeviceRegistryService(
            new DeviceInfoValidator(Clock.fixed(T0.plus(Duration.ofHours(1)), ZoneOffset.UTC),
                    new ValidationPolicy(Duration.ofDays(1))),
            new DeviceInfoMapper(), store, store,
            deviceId -> {
                resolvedShards.add(deviceId);
                return InMemoryDeviceRepository.shardOf(deviceId);
            },
            metrics, ObservationRegistry.NOOP, cache);

    @Test
    @DisplayName("First appearance: device inserted with seen_count 1, state from the snapshot, shard reported by storage")
    void givenNewDevice_whenProcess_thenInsertedWithSeenCountOne() {
        ProcessingResult result = process(info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0).build());

        assertThat(result).isEqualTo(new ProcessingResult(ProcessingOutcome.APPLIED, SHARD, 1L));
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.seenCount).isEqualTo(1);
        assertThat(row.status).isEqualTo("ACTIVE");
        assertThat(row.state.tenantId()).isEqualTo("tenant-1");
        assertThat(row.firstSeenAt).isEqualTo(T0);
        assertThat(row.lastEventAt).isEqualTo(T0);
        assertThat(row.lastEventId).isEqualTo("evt-1");
        assertThat(row.lastEventType).isEqualTo("CREATED");
        assertThat(store.journal(DEVICE)).singleElement()
                .satisfies(e -> assertThat(e.outcome()).isEqualTo(ProcessingOutcome.APPLIED));
        assertThat(counter("devices.messages.processed", "outcome", "applied")).isEqualTo(1);
        assertThat(registry.get("devices.db.write").tags("shard", SHARD, "result", "success").timer().count()).isEqualTo(1);
        // Шард взят из ответа шарда (RETURNING current_database()), Proxy не спрашивали
        assertThat(resolvedShards).isEmpty();
    }

    @Test
    @DisplayName("Repeated delivery of the same event: skipped, seen_count not increased")
    void givenSameEventTwice_whenProcess_thenDuplicateAndSeenCountUnchanged() {
        DeviceInfo message = info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0).build();
        process(message);

        ProcessingResult repeated = process(message);

        assertThat(repeated).isEqualTo(new ProcessingResult(ProcessingOutcome.DUPLICATE, SHARD, 1L));
        assertThat(store.device(DEVICE).seenCount).isEqualTo(1);
        assertThat(store.journal(DEVICE)).hasSize(1);
        assertThat(counter("devices.messages.duplicate")).isEqualTo(1);
        assertThat(counter("devices.messages.processed", "outcome", "applied")).isEqualTo(1);
    }

    @Test
    @DisplayName("Republished event (same eventId, new emittedAtMs) is still a duplicate")
    void givenRepublishedEvent_whenProcess_thenDuplicate() {
        process(info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0).build());

        ProcessingResult repeated = process(info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0)
                .setEmittedAtMs(T0.plusSeconds(30).toEpochMilli()).build());

        assertThat(repeated.outcome()).isEqualTo(ProcessingOutcome.DUPLICATE);
        assertThat(store.device(DEVICE).seenCount).isEqualTo(1);
    }

    @Test
    @DisplayName("DeviceInfo 1.0 repeated: duplicate by emittedAtMs; new device without snapshot has status UNKNOWN")
    void givenLegacyMessageTwice_whenProcess_thenDuplicateByEmittedAt() {
        ProcessingResult first = process(legacy(DEVICE, T0));
        ProcessingResult repeated = process(legacy(DEVICE, T0));

        assertThat(first.outcome()).isEqualTo(ProcessingOutcome.APPLIED);
        assertThat(repeated.outcome()).isEqualTo(ProcessingOutcome.DUPLICATE);
        assertThat(store.device(DEVICE).seenCount).isEqualTo(1);
        assertThat(store.device(DEVICE).status).isEqualTo("UNKNOWN");
        assertThat(store.device(DEVICE).state).isNull();
    }

    @Test
    @DisplayName("Newer event with snapshot: state replaced, seen_count + 1")
    void givenNewerSnapshot_whenProcess_thenStateUpdated() {
        process(info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0).build());

        ProcessingResult result = process(info(DEVICE, "evt-2", DeviceUpsertEventType.UPDATED, T0.plusSeconds(60))
                .setDevice(snapshot(DEVICE).setFirmwareVersion("2.0.0").setName("Renamed").setOsVersion("6.2")
                        .setStatus(DeviceStatus.MAINTENANCE).build())
                .build());

        assertThat(result.outcome()).isEqualTo(ProcessingOutcome.APPLIED);
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.seenCount).isEqualTo(2);
        assertThat(row.state.firmwareVersion()).isEqualTo("2.0.0");
        assertThat(row.state.name()).isEqualTo("Renamed");
        assertThat(row.state.osName()).isEqualTo("Linux");
        assertThat(row.state.osVersion()).isEqualTo("6.2");
        assertThat(row.status).isEqualTo("MAINTENANCE");
        assertThat(row.lastEventAt).isEqualTo(T0.plusSeconds(60));
        assertThat(row.lastEventId).isEqualTo("evt-2");
        assertThat(row.firstSeenAt).isEqualTo(T0);
    }

    @Test
    @DisplayName("Older event after a newer one: STALE, state not changed, seen_count + 1, first_seen_at moves back")
    void givenOlderEventAfterNewer_whenProcess_thenStaleOnlyCounted() {
        process(info(DEVICE, "evt-new", DeviceUpsertEventType.UPDATED, T0)
                .setDevice(snapshot(DEVICE).setFirmwareVersion("2.0.0").build()).build());

        ProcessingResult result = process(info(DEVICE, "evt-old", DeviceUpsertEventType.UPDATED, T0.minusSeconds(60))
                .setDevice(snapshot(DEVICE).setFirmwareVersion("1.0.0").build()).build());

        assertThat(result).isEqualTo(new ProcessingResult(ProcessingOutcome.STALE, SHARD, 1L));
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.seenCount).isEqualTo(2);
        assertThat(row.state.firmwareVersion()).as("newer state kept").isEqualTo("2.0.0");
        assertThat(row.lastEventAt).isEqualTo(T0);
        assertThat(row.lastEventId).isEqualTo("evt-new");
        assertThat(row.firstSeenAt).isEqualTo(T0.minusSeconds(60));
        assertThat(store.journal(DEVICE)).extracting(e -> e.eventKey() + ":" + e.outcome())
                .containsExactlyInAnyOrder("evt-new:APPLIED", "evt-old:STALE");
        assertThat(counter("devices.messages.processed", "outcome", "stale")).isEqualTo(1);
    }

    @Test
    @DisplayName("Stale event delivered again: duplicate, seen_count unchanged")
    void givenStaleEventTwice_whenProcess_thenSecondIsDuplicate() {
        process(info(DEVICE, "evt-new", DeviceUpsertEventType.UPDATED, T0).build());
        DeviceInfo old = info(DEVICE, "evt-old", DeviceUpsertEventType.UPDATED, T0.minusSeconds(60)).build();
        process(old);

        assertThat(process(old).outcome()).isEqualTo(ProcessingOutcome.DUPLICATE);
        assertThat(store.device(DEVICE).seenCount).isEqualTo(2);
    }

    @Test
    @DisplayName("Event without snapshot for a known device: model kept, last event updated, seen_count + 1")
    void givenEventWithoutSnapshot_whenProcess_thenOnlyEventFieldsUpdated() {
        process(info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0).build());

        process(info(DEVICE, "evt-2", DeviceUpsertEventType.SNAPSHOT, T0.plusSeconds(10)).setDevice(null).build());

        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.seenCount).isEqualTo(2);
        assertThat(row.status).isEqualTo("ACTIVE");
        assertThat(row.state.firmwareVersion()).isEqualTo("1.0.0");
        assertThat(row.lastEventAt).isEqualTo(T0.plusSeconds(10));
        assertThat(row.lastSeenAt).isEqualTo(T0.plusSeconds(10));
        assertThat(row.lastEventType).isEqualTo("SNAPSHOT");
    }

    @Test
    @DisplayName("last_seen_at is the latest of event time, snapshot lastSeenAt and the stored value")
    void givenSeveralActivityTimes_whenProcess_thenLatestLastSeenKept() {
        process(info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0)
                .setDevice(snapshot(DEVICE).setLastSeenAtMs(T0.plusSeconds(30).toEpochMilli()).build()).build());
        assertThat(store.device(DEVICE).lastSeenAt).as("from snapshot").isEqualTo(T0.plusSeconds(30));

        process(info(DEVICE, "evt-2", DeviceUpsertEventType.SNAPSHOT, T0.plusSeconds(10)).setDevice(null).build());
        assertThat(store.device(DEVICE).lastSeenAt).as("stored value is later").isEqualTo(T0.plusSeconds(30));

        process(info(DEVICE, "evt-3", DeviceUpsertEventType.SNAPSHOT, T0.plusSeconds(60)).setDevice(null).build());
        assertThat(store.device(DEVICE).lastSeenAt).as("event time is later").isEqualTo(T0.plusSeconds(60));
    }

    @Test
    @DisplayName("Event without snapshot after deletion keeps the device deleted")
    void givenDeletedDevice_whenEventWithoutSnapshot_thenStillDeleted() {
        process(info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0).build());
        process(info(DEVICE, "evt-2", DeviceUpsertEventType.DELETED, T0.plusSeconds(10)).setDevice(null).build());

        process(info(DEVICE, "evt-3", DeviceUpsertEventType.SNAPSHOT, T0.plusSeconds(20)).setDevice(null).build());

        assertThat(store.device(DEVICE).status).isEqualTo("DELETED");
        assertThat(store.device(DEVICE).deletedAt).isEqualTo(T0.plusSeconds(10));
    }

    @Test
    @DisplayName("DELETED: soft delete; a newer snapshot brings the device back")
    void givenDeletedThenNewerSnapshot_whenProcess_thenSoftDeletedAndRestored() {
        process(info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0).build());

        process(info(DEVICE, "evt-2", DeviceUpsertEventType.DELETED, T0.plusSeconds(10)).setDevice(null).build());
        assertThat(store.device(DEVICE).status).isEqualTo("DELETED");
        assertThat(store.device(DEVICE).deletedAt).isEqualTo(T0.plusSeconds(10));

        process(info(DEVICE, "evt-3", DeviceUpsertEventType.UPDATED, T0.plusSeconds(20)).build());
        assertThat(store.device(DEVICE).status).isEqualTo("ACTIVE");
        assertThat(store.device(DEVICE).deletedAt).isNull();
        assertThat(store.device(DEVICE).seenCount).isEqualTo(3);
    }

    @Test
    @DisplayName("Database error: nothing stored, shard resolved by Proxy, error and shard route failure counted")
    void givenStorageFailure_whenProcess_thenDeviceNotStoredWithShard() {
        store.failNextQuery(StorageFailureReason.CONNECTION);

        assertThatThrownBy(() -> process(info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0).build()))
                .isInstanceOfSatisfying(DeviceNotStoredException.class, e -> {
                    assertThat(e.shard()).isEqualTo(SHARD);
                    assertThat(e.reason()).isEqualTo(StorageFailureReason.CONNECTION);
                });

        assertThat(store.device(DEVICE)).isNull();
        assertThat(store.journal(DEVICE)).isEmpty();
        assertThat(resolvedShards).containsExactly(DEVICE);
        assertThat(counter("devices.processing.errors", "reason", "storage")).isEqualTo(1);
        // Сообщение завершается ошибкой только в devices-dlt (KafkaConfig), не на попытке
        assertThat(counter("devices.messages.failed", "reason", "storage")).isZero();
        assertThat(counter("devices.shard.route.fail", "shard", SHARD, "reason", "connection")).isEqualTo(1);
        assertThat(registry.get("devices.db.write").tags("shard", SHARD, "result", "error").timer().count()).isEqualTo(1);
    }

    @Test
    @DisplayName("Retry after a database error: the event is applied once")
    void givenStorageFailureThenRetry_whenProcess_thenAppliedOnce() {
        DeviceInfo message = info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0).build();
        store.failNextQuery(StorageFailureReason.TIMEOUT);
        assertThatThrownBy(() -> process(message)).isInstanceOf(DeviceNotStoredException.class);

        ProcessingResult retried = process(message);

        assertThat(retried.outcome()).isEqualTo(ProcessingOutcome.APPLIED);
        assertThat(store.device(DEVICE).seenCount).isEqualTo(1);
    }

    @Test
    @DisplayName("SQL error that is not a shard access problem is not counted as a shard route failure")
    void givenOtherSqlError_whenProcess_thenNoShardRouteFailure() {
        store.failNextQuery(StorageFailureReason.OTHER);

        assertThatThrownBy(() -> process(info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0).build()))
                .isInstanceOf(DeviceNotStoredException.class);

        assertThat(registry.find("devices.shard.route.fail").counters())
                .allSatisfy(c -> assertThat(c.count()).isZero());
        assertThat(counter("devices.processing.errors", "reason", "storage")).isEqualTo(1);
    }

    @Test
    @DisplayName("Invalid message: rejected before the database, counted as invalid")
    void givenInvalidMessage_whenProcess_thenRejectedWithoutStorage() {
        assertThatThrownBy(() -> service.process("other-key",
                info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0).build(), POSITION))
                .isInstanceOf(InvalidDeviceInfoException.class);

        assertThat(store.device(DEVICE)).isNull();
        assertThat(counter("devices.processing.errors", "reason", "invalid")).isEqualTo(1);
        assertThat(registry.get("devices.processing").tag("outcome", "invalid").timer().count()).isEqualTo(1);
    }

    @Test
    @DisplayName("Messages, not attempts: received = processed + duplicate + failed; failed attempts counted separately")
    void givenMixedDeliveries_whenProcess_thenCountersAddUp() {
        DeviceInfo first = info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0).build();
        process(first);
        process(first);
        process(info(DEVICE, "evt-0", DeviceUpsertEventType.UPDATED, T0.minusSeconds(5)).build());
        DeviceInfo failing = info(DEVICE, "evt-2", DeviceUpsertEventType.UPDATED, T0.plusSeconds(5)).build();
        for (int attempt = 1; attempt <= 3; attempt++) {
            store.failNextQuery(StorageFailureReason.CONNECTION);
            MessagePosition delivery = new MessagePosition("devices", 1, 43L, attempt);
            assertThatThrownBy(() -> service.process(DEVICE, failing, delivery)).isInstanceOf(DeviceNotStoredException.class);
        }
        // После последней попытки обработчик ошибок (KafkaConfig) отправляет сообщение в devices-dlt
        metrics.failed(FailureReason.STORAGE);

        double processed = registry.find("devices.messages.processed").counters().stream().mapToDouble(c -> c.count()).sum();
        double failed = registry.find("devices.messages.failed").counters().stream().mapToDouble(c -> c.count()).sum();
        assertThat(counter("devices.messages.received")).isEqualTo(4);
        assertThat(processed + counter("devices.messages.duplicate") + failed).isEqualTo(4);
        assertThat(counter("devices.processing.errors", "reason", "storage")).isEqualTo(3);
    }

    @Test
    @DisplayName("Snapshot older than telemetry already seen: applied, last event fields kept from the later event")
    void givenSnapshotOlderThanTelemetry_whenProcess_thenStateApplied() {
        process(info(DEVICE, "evt-telemetry", DeviceUpsertEventType.CREATED, T0.plusSeconds(10)).setDevice(null).build());

        ProcessingResult result = process(info(DEVICE, "evt-registration", DeviceUpsertEventType.UPDATED, T0).build());

        assertThat(result.outcome()).isEqualTo(ProcessingOutcome.APPLIED);
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.state.tenantId()).isEqualTo("tenant-1");
        assertThat(row.status).isEqualTo("ACTIVE");
        assertThat(row.seenCount).isEqualTo(2);
        assertThat(row.lastStateAt).isEqualTo(T0);
        assertThat(row.lastEventAt).isEqualTo(T0.plusSeconds(10));
        assertThat(row.lastEventId).isEqualTo("evt-telemetry");
        assertThat(row.firstSeenAt).isEqualTo(T0);
    }

    @Test
    @DisplayName("DeviceInfo 1.0 (time of publication) does not hide an older 1.1 snapshot")
    void givenLegacyMessageThenOlderSnapshot_whenProcess_thenStateApplied() {
        process(legacy(DEVICE, T0.plusSeconds(30)));

        ProcessingResult result = process(info(DEVICE, "evt-1", DeviceUpsertEventType.UPDATED, T0).build());

        assertThat(result.outcome()).isEqualTo(ProcessingOutcome.APPLIED);
        assertThat(store.device(DEVICE).state.tenantId()).isEqualTo("tenant-1");
        assertThat(store.device(DEVICE).lastEventAt).isEqualTo(T0.plusSeconds(30));
    }

    @Test
    @DisplayName("Deletion older than the applied snapshot: STALE, device stays active")
    void givenDeletionOlderThanState_whenProcess_thenStale() {
        process(info(DEVICE, "evt-2", DeviceUpsertEventType.UPDATED, T0.plusSeconds(10)).build());

        ProcessingResult result = process(info(DEVICE, "evt-1", DeviceUpsertEventType.DELETED, T0).setDevice(null).build());

        assertThat(result.outcome()).isEqualTo(ProcessingOutcome.STALE);
        assertThat(store.device(DEVICE).status).isEqualTo("ACTIVE");
        assertThat(store.device(DEVICE).deletedAt).isNull();
    }

    @Test
    @DisplayName("Failure after the journal row is written: whole transaction rolled back, retry applies the event once")
    void givenFailureAfterJournal_whenRetry_thenAppliedOnce() {
        DeviceInfo message = info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0).build();
        // 1 - SELECT FOR UPDATE, 2 - INSERT в журнал, 3 - INSERT devices
        store.failQuery(3, StorageFailureReason.CONNECTION);
        assertThatThrownBy(() -> process(message)).isInstanceOf(DeviceNotStoredException.class);
        assertThat(store.journal(DEVICE)).as("journal rolled back with the device").isEmpty();

        ProcessingResult retried = process(message);

        assertThat(retried.outcome()).isEqualTo(ProcessingOutcome.APPLIED);
        assertThat(store.device(DEVICE).seenCount).isEqualTo(1);
        assertThat(store.journal(DEVICE)).hasSize(1);
    }

    // ------------------------------------------------------------------ правила команд (FR-015, FR-016, FR-017)
    // Строки таблицы FR-015 в spec.md: по одному тесту на строку (1-11), FR-017 - отдельный тест

    @Test
    @DisplayName("FR-015 строка 1: CREATE, устройства нет - карточка создана, managed_at = время команды")
    void givenCreate_whenNoDevice_thenInserted() {
        ProcessingResult result = process(command(DEVICE, "cmd-1", OperationType.CREATE, T0).build());

        assertThat(result).isEqualTo(new ProcessingResult(ProcessingOutcome.APPLIED, SHARD, 1L));
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.status).isEqualTo("ACTIVE");
        assertThat(row.state.tenantId()).isEqualTo("tenant-1");
        assertThat(row.state.name()).isEqualTo("Boiler room sensor");
        assertThat(row.state.firmwareVersion()).as("команда не несёт данных устройства").isNull();
        assertThat(row.managedAt).isEqualTo(T0);
        assertThat(row.deletedAt).isNull();
        assertThat(row.deletedBy).isNull();
        assertThat(row.version).isEqualTo(1L);
        assertThat(row.seenCount).isEqualTo(1);
    }

    @Test
    @DisplayName("FR-015 строка 1: CREATE для удалённого устройства - удаление снято, поля администратора заменены")
    void givenCreate_whenDeleted_thenRestored() {
        process(command(DEVICE, "cmd-1", OperationType.DELETE, T0).build());

        ProcessingResult result = process(command(DEVICE, "cmd-2", OperationType.CREATE, T0.plusSeconds(10))
                .setDevice(adminSnapshot(DEVICE).setName("Restored").build()).build());

        assertThat(result.outcome()).isEqualTo(ProcessingOutcome.APPLIED);
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.status).isEqualTo("ACTIVE");
        assertThat(row.deletedAt).isNull();
        assertThat(row.deletedBy).isNull();
        assertThat(row.state.name()).isEqualTo("Restored");
        assertThat(row.managedAt).isEqualTo(T0.plusSeconds(10));
    }

    @Test
    @DisplayName("FR-015 строка 2: UPDATE, устройства нет - карточка создана")
    void givenUpdate_whenNoDevice_thenInserted() {
        ProcessingResult result = process(command(DEVICE, "cmd-1", OperationType.UPDATE, T0).build());

        assertThat(result).isEqualTo(new ProcessingResult(ProcessingOutcome.APPLIED, SHARD, 1L));
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.state.tenantId()).isEqualTo("tenant-1");
        assertThat(row.status).isEqualTo("ACTIVE");
        assertThat(row.managedAt).isEqualTo(T0);
        assertThat(row.seenCount).isEqualTo(1);
    }

    @Test
    @DisplayName("FR-015 строка 3: UPDATE, устройство есть - поля администратора заменены, данные устройства сохранены")
    void givenUpdate_whenExists_thenAdminFieldsReplaced() {
        process(info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0)
                .setDevice(snapshot(DEVICE).setFirmwareVersion("2.0.0").setName("Router name").build()).build());

        ProcessingResult result = process(command(DEVICE, "cmd-1", OperationType.UPDATE, T0.plusSeconds(10))
                .setDevice(adminSnapshot(DEVICE).setName("Renamed").setStatus(DeviceStatus.MAINTENANCE)
                        .setSettings(Map.of("mode", "boost")).build())
                .build());

        assertThat(result.outcome()).isEqualTo(ProcessingOutcome.APPLIED);
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.state.name()).as("поле администратора заменено").isEqualTo("Renamed");
        assertThat(row.status).isEqualTo("MAINTENANCE");
        assertThat(row.state.settings()).containsEntry("mode", "boost");
        assertThat(row.state.firmwareVersion()).as("данные устройства не трогает команда").isEqualTo("2.0.0");
        assertThat(row.state.osName()).isEqualTo("Linux");
        assertThat(row.state.sensors()).hasSize(1);
        assertThat(row.managedAt).isEqualTo(T0.plusSeconds(10));
    }

    @Test
    @DisplayName("FR-015 строка 4: UPDATE для удалённого устройства - без изменений, IGNORED")
    void givenUpdate_whenDeleted_thenIgnored() {
        process(command(DEVICE, "cmd-1", OperationType.CREATE, T0).build());
        process(command(DEVICE, "cmd-2", OperationType.DELETE, T0.plusSeconds(10)).build());

        ProcessingResult result = process(command(DEVICE, "cmd-3", OperationType.UPDATE, T0.plusSeconds(20))
                .setDevice(adminSnapshot(DEVICE).setName("Ignored").build()).build());

        assertThat(result).isEqualTo(new ProcessingResult(ProcessingOutcome.IGNORED, SHARD, 2L));
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.status).isEqualTo("DELETED");
        assertThat(row.deletedAt).isEqualTo(T0.plusSeconds(10));
        assertThat(row.state.name()).isEqualTo("Boiler room sensor");
        assertThat(row.seenCount).as("учтено в seen_count").isEqualTo(3);
        assertThat(store.journal(DEVICE)).extracting(e -> e.eventKey() + ":" + e.outcome())
                .contains("cmd-3:IGNORED");
    }

    @Test
    @DisplayName("FR-015 строка 5: DELETE, устройство есть - помечено удалённым администратором, managed_at = время команды")
    void givenDelete_whenExists_thenDeletedByAdmin() {
        process(info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0).build());

        ProcessingResult result = process(command(DEVICE, "cmd-1", OperationType.DELETE, T0.plusSeconds(10)).build());

        assertThat(result).isEqualTo(new ProcessingResult(ProcessingOutcome.APPLIED, SHARD, 2L));
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.status).isEqualTo("DELETED");
        assertThat(row.deletedAt).isEqualTo(T0.plusSeconds(10));
        assertThat(row.deletedBy).isEqualTo("ADMIN");
        assertThat(row.managedAt).isEqualTo(T0.plusSeconds(10));
    }

    @Test
    @DisplayName("FR-015 строка 5: DELETE, устройства нет - надгробие: удалено, версия 1, состояние с времени команды")
    void givenDelete_whenNoDevice_thenTombstone() {
        ProcessingResult result = process(command(DEVICE, "cmd-1", OperationType.DELETE, T0).build());

        assertThat(result).isEqualTo(new ProcessingResult(ProcessingOutcome.APPLIED, SHARD, 1L));
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.status).isEqualTo("DELETED");
        assertThat(row.deletedAt).isEqualTo(T0);
        assertThat(row.deletedBy).isEqualTo("ADMIN");
        assertThat(row.managedAt).isEqualTo(T0);
        assertThat(row.lastStateAt).isEqualTo(T0);
        assertThat(row.lastEventType).isEqualTo("DELETED");
        assertThat(row.state).as("данных устройства нет").isNull();
        assertThat(row.seenCount).isEqualTo(1);
    }

    @Test
    @DisplayName("FR-015 строка 6: команда старее последней применённой - STALE, поля не меняются")
    void givenOlderCommand_whenNewerApplied_thenStale() {
        process(command(DEVICE, "cmd-2", OperationType.UPDATE, T0)
                .setDevice(adminSnapshot(DEVICE).setName("New").build()).build());

        ProcessingResult result = process(command(DEVICE, "cmd-1", OperationType.UPDATE, T0.minusSeconds(60))
                .setDevice(adminSnapshot(DEVICE).setName("Old").build()).build());

        assertThat(result).isEqualTo(new ProcessingResult(ProcessingOutcome.STALE, SHARD, 1L));
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.state.name()).isEqualTo("New");
        assertThat(row.managedAt).isEqualTo(T0);
        assertThat(row.seenCount).isEqualTo(2);
        assertThat(store.journal(DEVICE)).extracting(e -> e.eventKey() + ":" + e.outcome())
                .contains("cmd-1:STALE");
    }

    @Test
    @DisplayName("FR-015 строка 6: запоздавшая CREATE после DELETE - STALE, устройство остаётся удалённым")
    void givenLateCreate_whenDeleteApplied_thenStaleAndStillDeleted() {
        // Замечание ревью: DELETE тоже ставит managed_at, иначе команда с более ранним временем воскресила бы устройство
        process(command(DEVICE, "cmd-1", OperationType.CREATE, T0).build());
        process(command(DEVICE, "cmd-2", OperationType.DELETE, T0.plusSeconds(20)).build());

        ProcessingResult result = process(command(DEVICE, "cmd-3", OperationType.CREATE, T0.plusSeconds(15)).build());

        assertThat(result.outcome()).isEqualTo(ProcessingOutcome.STALE);
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.status).isEqualTo("DELETED");
        assertThat(row.deletedBy).isEqualTo("ADMIN");
        assertThat(row.managedAt).isEqualTo(T0.plusSeconds(20));
        assertThat(store.journal(DEVICE)).extracting(e -> e.eventKey() + ":" + e.outcome())
                .contains("cmd-3:STALE");
    }

    @Test
    @DisplayName("FR-015 строка 7: событие роутера для устройства, удалённого администратором - IGNORED")
    void givenRouterEvent_whenDeletedByAdmin_thenIgnored() {
        process(command(DEVICE, "cmd-1", OperationType.CREATE, T0).build());
        process(command(DEVICE, "cmd-2", OperationType.DELETE, T0.plusSeconds(10)).build());

        ProcessingResult result = process(info(DEVICE, "evt-1", DeviceUpsertEventType.UPDATED, T0.plusSeconds(20)).build());

        assertThat(result.outcome()).isEqualTo(ProcessingOutcome.IGNORED);
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.status).isEqualTo("DELETED");
        assertThat(row.deletedBy).isEqualTo("ADMIN");
        assertThat(row.seenCount).isEqualTo(3);
    }

    @Test
    @DisplayName("FR-015 строка 8: событие роутера для устройства с ручными изменениями - только данные устройства")
    void givenRouterEvent_whenManaged_thenOnlyDeviceDataUpdated() {
        process(command(DEVICE, "cmd-1", OperationType.CREATE, T0).build());

        ProcessingResult result = process(info(DEVICE, "evt-1", DeviceUpsertEventType.UPDATED, T0.plusSeconds(20))
                .setDevice(snapshot(DEVICE).setTenantId("router-tenant").setFirmwareVersion("2.0.0")
                        .setName("Router name").setStatus(DeviceStatus.MAINTENANCE)
                        .setSettings(Map.of("mode", "router")).build())
                .build());

        assertThat(result.outcome()).isEqualTo(ProcessingOutcome.APPLIED);
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.state.firmwareVersion()).as("данные устройства обновлены").isEqualTo("2.0.0");
        assertThat(row.state.tenantId()).as("поля администратора не меняются").isEqualTo("tenant-1");
        assertThat(row.state.name()).isEqualTo("Boiler room sensor");
        assertThat(row.status).isEqualTo("ACTIVE");
        assertThat(row.state.settings()).containsEntry("mode", "eco");
        assertThat(row.version).isEqualTo(2L);
    }

    @Test
    @DisplayName("FR-015 строка 9: событие роутера без ручных изменений - как раньше, поля устройства replaced")
    void givenRouterEvent_whenNotManaged_thenAppliedAsBefore() {
        process(info(DEVICE, "evt-1", DeviceUpsertEventType.CREATED, T0).build());

        ProcessingResult result = process(info(DEVICE, "evt-2", DeviceUpsertEventType.UPDATED, T0.plusSeconds(10))
                .setDevice(snapshot(DEVICE).setName("Router renamed").setStatus(DeviceStatus.MAINTENANCE).build())
                .build());

        assertThat(result.outcome()).isEqualTo(ProcessingOutcome.APPLIED);
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.state.name()).isEqualTo("Router renamed");
        assertThat(row.status).isEqualTo("MAINTENANCE");
        assertThat(row.managedAt).isNull();
        assertThat(row.deletedBy).isNull();
    }

    @Test
    @DisplayName("FR-015 строка 12: DELETE от роутера без снимка для управляемого устройства - IGNORED, карточка и кэш не меняются")
    void givenRouterDeleteWithoutSnapshot_whenManaged_thenIgnored() {
        process(command(DEVICE, "cmd-1", OperationType.CREATE, T0).build());

        ProcessingResult result = process(info(DEVICE, "evt-1", DeviceUpsertEventType.DELETED, T0.plusSeconds(10))
                .setDevice(null).build());

        assertThat(result).isEqualTo(new ProcessingResult(ProcessingOutcome.IGNORED, SHARD, 1L));
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.status).isEqualTo("ACTIVE");
        assertThat(row.deletedAt).isNull();
        assertThat(row.deletedBy).isNull();
        assertThat(row.state.name()).isEqualTo("Boiler room sensor");
        assertThat(row.version).as("версия не растёт").isEqualTo(1L);
        assertThat(row.seenCount).as("учтено в seen_count").isEqualTo(2);
        assertThat(store.journal(DEVICE)).extracting(e -> e.eventKey() + ":" + e.outcome())
                .contains("evt-1:IGNORED");
        assertThat(raisedFloors).as("кэш не сбрасывается").containsExactly(DEVICE + "@1");
    }

    @Test
    @DisplayName("FR-015 строка 10: повтор уже обработанной команды - DUPLICATE, ничего не меняется")
    void givenSameCommandTwice_whenProcess_thenDuplicate() {
        DeviceInfo message = command(DEVICE, "cmd-1", OperationType.UPDATE, T0).build();
        process(message);

        ProcessingResult repeated = process(message);

        assertThat(repeated).isEqualTo(new ProcessingResult(ProcessingOutcome.DUPLICATE, SHARD, 1L));
        assertThat(store.device(DEVICE).seenCount).isEqualTo(1);
        assertThat(store.journal(DEVICE)).hasSize(1);
    }

    @Test
    @DisplayName("FR-017: событие роутера новее команды не делает команду устаревшей - команда применяется")
    void givenRouterClockAhead_whenAdminUpdate_thenApplied() {
        process(info(DEVICE, "evt-router", DeviceUpsertEventType.UPDATED, T0.plusSeconds(3600)).build());

        ProcessingResult result = process(command(DEVICE, "cmd-1", OperationType.UPDATE, T0)
                .setDevice(adminSnapshot(DEVICE).setName("Admin").build()).build());

        assertThat(result.outcome()).isEqualTo(ProcessingOutcome.APPLIED);
        InMemoryDeviceRepository.Row row = store.device(DEVICE);
        assertThat(row.state.name()).isEqualTo("Admin");
        assertThat(row.managedAt).isEqualTo(T0);
        assertThat(row.lastEventAt).isEqualTo(T0.plusSeconds(3600));
    }

    // ------------------------------------------------------------------ кэш устройства (FR-018, FR-021)

    @Test
    @DisplayName("FR-018: applied change - floor raised after the commit with the applied version, metric success")
    void givenAppliedChange_whenProcess_thenFloorRaisedWithAppliedVersion() {
        process(command(DEVICE, "cmd-1", OperationType.CREATE, T0).build());
        process(command(DEVICE, "cmd-2", OperationType.UPDATE, T0.plusSeconds(10)).build());

        assertThat(raisedFloors).containsExactly(DEVICE + "@1", DEVICE + "@2");
        assertThat(counter("devices.cache.invalidations", "result", "success")).isEqualTo(2);
    }

    @Test
    @DisplayName("FR-018: stale messages do not touch the cache")
    void givenStaleMessage_whenProcess_thenFloorNotRaised() {
        process(command(DEVICE, "cmd-1", OperationType.UPDATE, T0).build());

        process(command(DEVICE, "cmd-0", OperationType.UPDATE, T0.minusSeconds(60)).build());

        assertThat(raisedFloors).containsExactly(DEVICE + "@1");
    }

    @Test
    @DisplayName("FR-018: repeated delivery after a failed cache reset raises the floor to the row version")
    void givenCacheResetFailedAfterCommit_whenRedelivered_thenFloorRaisedOnDuplicate() {
        DeviceInfo applied = command(DEVICE, "cmd-1", OperationType.CREATE, T0).build();
        // Сбой между COMMIT и сбросом: сообщение применено, кэш не сброшен, ack не отправлен
        redisDown = true;
        process(applied);
        redisDown = false;

        ProcessingResult repeated = process(applied);

        assertThat(repeated).isEqualTo(new ProcessingResult(ProcessingOutcome.DUPLICATE, SHARD, 1L));
        assertThat(raisedFloors).containsExactly(DEVICE + "@1", DEVICE + "@1");
        assertThat(counter("devices.cache.invalidations", "result", "success")).isEqualTo(1);
        assertThat(store.device(DEVICE).version).isEqualTo(1L);
    }

    @Test
    @DisplayName("FR-021: Redis unavailable - the message is applied and its result is unchanged, error counted")
    void givenRedisDown_whenProcess_thenResultUnchanged() {
        redisDown = true;

        ProcessingResult result = process(command(DEVICE, "cmd-1", OperationType.CREATE, T0).build());

        assertThat(result).isEqualTo(new ProcessingResult(ProcessingOutcome.APPLIED, SHARD, 1L));
        assertThat(store.device(DEVICE).version).isEqualTo(1L);
        assertThat(counter("devices.cache.invalidations", "result", "error")).isEqualTo(1);
    }

    private ProcessingResult process(DeviceInfo message) {
        return service.process(message.getDeviceId(), message, POSITION);
    }

    private double counter(String name, String... tags) {
        return registry.get(name).tags(tags).counter().count();
    }
}