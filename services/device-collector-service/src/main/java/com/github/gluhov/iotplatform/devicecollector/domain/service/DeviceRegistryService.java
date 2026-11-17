package com.github.gluhov.iotplatform.devicecollector.domain.service;

import com.github.gluhov.iotplatform.devicecollector.domain.exception.DeviceNotStoredException;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.StorageAccessException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.CommandType;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceChange;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceRecord;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceState;
import com.github.gluhov.iotplatform.devicecollector.domain.model.FailureReason;
import com.github.gluhov.iotplatform.devicecollector.domain.model.MessagePosition;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessedEvent;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessingOutcome;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessingResult;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StoredDevice;
import com.github.gluhov.iotplatform.devicecollector.domain.model.WrittenDevice;
import com.github.gluhov.iotplatform.devicecollector.domain.port.in.ProcessDeviceInfoUseCase;
import com.github.gluhov.iotplatform.devicecollector.domain.port.out.DeviceCacheInvalidator;
import com.github.gluhov.iotplatform.devicecollector.domain.port.out.DeviceMetrics;
import com.github.gluhov.iotplatform.devicecollector.domain.port.out.DeviceRepository;
import com.github.gluhov.iotplatform.devicecollector.domain.port.out.ShardResolver;
import com.github.gluhov.iotplatform.devicecollector.domain.port.out.TransactionRunner;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Применение DeviceInfo к реестру - одна локальная транзакция на шарде устройства:
 * <ol>
 *   <li>SELECT ... FOR UPDATE строки устройства - события одного устройства обрабатываются строго по очереди;</li>
 *   <li>исход (outcomeOf) по таблице правил (data-model.md, FR-015): команда сравнивается с managed_at,
 *       событие роутера - с last_state_at (как раньше); событие роутера не применяется к устройству,
 *       удалённому администратором; UPDATE для удалённого устройства не применяется;</li>
 *   <li>INSERT в журнал ON CONFLICT DO NOTHING: 0 строк - повторная доставка, ничего не меняется
 *       (ON CONFLICT DO UPDATE на devices от повтора не защищает: увеличил бы seen_count);</li>
 *   <li>APPLIED - вставка или запись устройства, STALE и IGNORED - только seen_count + 1;</li>
 *   <li>после COMMIT для APPLIED и DUPLICATE - сброс кэша устройства (порог до версии строки); ошибка кэша результат не меняет.</li>
 * </ol>
 * Журнал и устройство - связанные таблицы с одним ключом шардирования, поэтому транзакция не распределённая.
 */
@Service
@RequiredArgsConstructor
public class DeviceRegistryService implements ProcessDeviceInfoUseCase {
    private final DeviceInfoValidator validator;
    private final DeviceInfoMapper mapper;
    private final DeviceRepository repository;
    private final TransactionRunner transactions;
    private final ShardResolver shardResolver;
    private final DeviceMetrics metrics;
    private final ObservationRegistry observationRegistry;
    private final DeviceCacheInvalidator cache;

    @Override
    public ProcessingResult process(String key, DeviceInfo info, MessagePosition position) {
        long started = System.nanoTime();
        if (position.firstDelivery()) {
            // Повтор после ошибки - то же сообщение: получено один раз
            metrics.received();
        }
        String outcome = FailureReason.ERROR.label();
        try {
            step("validate").observe(() -> validator.validate(key, info));
            DeviceChange change = mapper.toChange(info, position);
            ProcessingResult result = store(change);
            metrics.processed(result.outcome());
            if (result.outcome() == ProcessingOutcome.APPLIED || result.outcome() == ProcessingOutcome.DUPLICATE) {
                // После COMMIT: кэш не должен отдавать данные старее применённой версии. DUPLICATE - повтор после сбоя
                // между COMMIT и сбросом: порог поднимаем до версии строки. Сбой кэша - не сбой обработки
                metrics.cacheInvalidated(cache.raiseFloor(change.deviceId(), result.version()));
            }
            outcome = result.outcome().label();
            return result;
        } catch (RuntimeException e) {
            FailureReason reason = FailureReason.of(e);
            // Попытка; сообщение завершается ошибкой (failed) только при отправке в devices-dlt - KafkaConfig
            metrics.processingError(reason);
            outcome = reason.label();
            throw e;
        } finally {
            metrics.processingTime(outcome, elapsed(started));
        }
    }

    private ProcessingResult store(DeviceChange change) {
        long started = System.nanoTime();
        Observation transaction = step("transaction").start();
        try (Observation.Scope scope = transaction.openScope()) {
            ProcessingResult result = transactions.inTransaction(() -> apply(change));
            transaction.highCardinalityKeyValue("shard", result.shard());
            metrics.dbWrite(result.shard(), true, elapsed(started));
            return result;
        } catch (StorageAccessException e) {
            // Ответа шарда нет - шард спрашиваем у Proxy (PREVIEW, к самому шарду не обращается)
            String shard = shardResolver.shardOf(change.deviceId());
            transaction.highCardinalityKeyValue("shard", shard);
            transaction.error(e);
            metrics.dbWrite(shard, false, elapsed(started));
            if (e.reason().isShardAccess()) {
                metrics.shardRouteFailed(shard, e.reason());
            }
            throw new DeviceNotStoredException(change.deviceId(), shard, e);
        } catch (RuntimeException e) {
            transaction.error(e);
            metrics.dbWrite(ProcessingResult.UNKNOWN_SHARD, false, elapsed(started));
            throw e;
        } finally {
            transaction.stop();
        }
    }

    private ProcessingResult apply(DeviceChange change) {
        Optional<StoredDevice> stored = step("select device for update")
                .observe(() -> repository.lockById(change.deviceId()));
        StoredDevice current = stored.orElse(null);
        ProcessingOutcome outcome = outcomeOf(change, current);
        boolean firstDelivery = Boolean.TRUE.equals(step("insert processed_device_events")
                .observe(() -> repository.markProcessed(ProcessedEvent.of(change, outcome))));
        if (!firstDelivery) {
            // Журнал и устройство пишутся в одной транзакции: событие в журнале - значит, устройство есть
            return new ProcessingResult(ProcessingOutcome.DUPLICATE, shardOf(current), versionOf(current));
        }
        WrittenDevice written;
        if (outcome == ProcessingOutcome.STALE || outcome == ProcessingOutcome.IGNORED) {
            written = step("update devices: count only")
                    .observe(() -> repository.countStale(change.deviceId(), change.eventAt()));
        } else if (current == null) {
            written = step("insert devices").observe(() -> repository.insert(toRecord(change, null)));
        } else if (change.isCommand()) {
            written = step("update devices: command")
                    .observe(() -> repository.updateAdmin(toRecord(change, current)));
        } else if (current.managed()) {
            written = step("update devices: managed device event")
                    .observe(() -> repository.updateDeviceData(toManagedRecord(change, current)));
        } else if (change.hasState()) {
            written = step("update devices: snapshot")
                    .observe(() -> repository.updateWithState(toRecord(change, current)));
        } else {
            written = step("update devices: event")
                    .observe(() -> repository.updateWithoutState(toRecord(change, current)));
        }
        return new ProcessingResult(outcome, written.shard(), written.version());
    }

    /**
     * Исход по таблице правил FR-015 (строки 1-9; строки 10 и 11 - повтор журнала и невалидное сообщение, решают
     * другие части обработки). Команда сравнивается с managed_at: строго раньше - STALE. Событие роутера для
     * устройства, удалённого администратором, - IGNORED; DELETE роутера без снимка для управляемого устройства - IGNORED.
     * Событие роутера сравнивается с last_state_at (снимок или удаление) или с last_event_at (без состояния), как раньше.
     * Равное время - APPLIED.
     */
    static ProcessingOutcome outcomeOf(DeviceChange change, StoredDevice stored) {
        if (stored == null) {
            return ProcessingOutcome.APPLIED;
        }
        if (change.isCommand()) {
            if (stored.managedAt() != null && change.eventAt().isBefore(stored.managedAt())) {
                return ProcessingOutcome.STALE;
            }
            if (change.command() == CommandType.UPDATE && stored.deletedAt() != null) {
                return ProcessingOutcome.IGNORED;
            }
            return ProcessingOutcome.APPLIED;
        }
        if (stored.deletedByAdmin()) {
            return ProcessingOutcome.IGNORED;
        }
        if (stored.managedAt() != null && change.deletes() && !change.hasState()) {
            // Удаляет только администратор: DELETE роутера без снимка для управляемого устройства не применяется
            return ProcessingOutcome.IGNORED;
        }
        if (change.changesState()) {
            return stored.lastStateAt() == null || !change.eventAt().isBefore(stored.lastStateAt())
                    ? ProcessingOutcome.APPLIED
                    : ProcessingOutcome.STALE;
        }
        return change.eventAt().isBefore(stored.lastEventAt()) ? ProcessingOutcome.STALE : ProcessingOutcome.APPLIED;
    }

    /** Запись строки devices; stored - текущая строка или null для нового устройства. */
    static DeviceRecord toRecord(DeviceChange change, StoredDevice stored) {
        if (change.isCommand()) {
            return commandRecord(change, stored);
        }
        DeviceState state = change.state();
        String status;
        Instant deletedAt;
        String deletedBy;
        if (change.deletes()) {
            status = DeviceState.DELETED;
            deletedAt = change.eventAt();
            deletedBy = StoredDevice.DELETED_BY_DEVICE;
        } else if (state != null) {
            // Более новое событие со снимком после удаления роутером возвращает устройство
            status = state.status();
            deletedAt = null;
            deletedBy = null;
        } else if (stored != null) {
            status = stored.status();
            deletedAt = stored.deletedAt();
            deletedBy = stored.deletedBy();
        } else {
            status = DeviceState.UNKNOWN;
            deletedAt = null;
            deletedBy = null;
        }
        return new DeviceRecord(change.deviceId(), state, status, lastSeenAt(change, state, stored), deletedAt, deletedBy,
                change.eventAt(), change.eventId(), change.type(), change.changesState() ? change.eventAt() : null, null);
    }

    /**
     * Событие роутера для устройства с ручными изменениями: состояние - только данные устройства. Время состояния
     * пишется только со снимком: удаление без снимка не меняет данные устройства.
     */
    static DeviceRecord toManagedRecord(DeviceChange change, StoredDevice stored) {
        DeviceRecord base = toRecord(change, stored);
        return new DeviceRecord(base.deviceId(), base.state(), base.status(), base.lastSeenAt(), base.deletedAt(),
                base.deletedBy(), base.eventAt(), base.eventId(), base.eventType(),
                change.hasState() ? change.eventAt() : null, null);
    }

    /**
     * Команда администратора: снимок несёт только поля администратора; DELETE удаляет мягко и помечает ADMIN.
     * Надгробие (устройства нет, DELETE) получает время состояния, как в data-model.md.
     */
    private static DeviceRecord commandRecord(DeviceChange change, StoredDevice stored) {
        DeviceState state = change.state();
        boolean deletes = change.command() == CommandType.DELETE;
        String status;
        if (deletes) {
            status = DeviceState.DELETED;
        } else {
            status = state == null ? DeviceState.UNKNOWN : state.status();
        }
        Instant deletedAt = deletes ? change.eventAt() : null;
        String deletedBy = deletes ? StoredDevice.DELETED_BY_ADMIN : null;
        Instant stateAt = deletes && stored == null ? change.eventAt() : null;
        return new DeviceRecord(change.deviceId(), state, status, null, deletedAt, deletedBy,
                change.eventAt(), change.eventId(), change.type(), stateAt, change.eventAt());
    }

    /** Последняя известная активность: время события, время из снимка и уже сохранённое - что позже. */
    private static Instant lastSeenAt(DeviceChange change, DeviceState state, StoredDevice stored) {
        return Stream.of(change.eventAt(), state == null ? null : state.lastSeenAt(),
                        stored == null ? null : stored.lastSeenAt())
                .filter(Objects::nonNull)
                .max(Instant::compareTo)
                .orElse(change.eventAt());
    }

    private static String shardOf(StoredDevice stored) {
        return stored == null ? ProcessingResult.UNKNOWN_SHARD : stored.shard();
    }

    private static long versionOf(StoredDevice stored) {
        return stored == null ? 0L : stored.version();
    }

    private Observation step(String name) {
        return Observation.createNotStarted("devices.process.step", observationRegistry)
                .contextualName(name)
                .lowCardinalityKeyValue("step", name);
    }

    private static Duration elapsed(long startedNanos) {
        return Duration.ofNanos(System.nanoTime() - startedNanos);
    }
}
