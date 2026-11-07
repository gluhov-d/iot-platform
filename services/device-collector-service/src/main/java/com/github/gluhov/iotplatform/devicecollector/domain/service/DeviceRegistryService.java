package com.github.gluhov.iotplatform.devicecollector.domain.service;

import com.github.gluhov.iotplatform.devicecollector.domain.exception.DeviceNotStoredException;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.StorageAccessException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceChange;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceRecord;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceState;
import com.github.gluhov.iotplatform.devicecollector.domain.model.FailureReason;
import com.github.gluhov.iotplatform.devicecollector.domain.model.MessagePosition;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessedEvent;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessingOutcome;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessingResult;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StoredDevice;
import com.github.gluhov.iotplatform.devicecollector.domain.port.in.ProcessDeviceInfoUseCase;
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
 *   <li>исход (outcomeOf): снимок или удаление старше состояния строки (last_state_at) - STALE; событие без
 *       состояния (сообщение 1.0.0, событие без снимка) старше последнего события (last_event_at) - STALE;
 *       иначе APPLIED. Снимок регистрации, пришедший после более поздней телеметрии, поэтому применяется;</li>
 *   <li>INSERT в журнал ON CONFLICT DO NOTHING: 0 строк - повторная доставка, ничего не меняется
 *       (ON CONFLICT DO UPDATE на devices от повтора не защищает: увеличил бы seen_count);</li>
 *   <li>APPLIED - вставка или обновление устройства, STALE - только seen_count + 1.</li>
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
            ProcessingResult result = store(mapper.toChange(info, position));
            metrics.processed(result.outcome());
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
        ProcessingOutcome outcome = outcomeOf(change, stored.orElse(null));
        boolean firstDelivery = Boolean.TRUE.equals(step("insert processed_device_events")
                .observe(() -> repository.markProcessed(ProcessedEvent.of(change, outcome))));
        if (!firstDelivery) {
            // Журнал и устройство пишутся в одной транзакции: событие в журнале - значит, устройство есть
            return new ProcessingResult(ProcessingOutcome.DUPLICATE,
                    stored.map(StoredDevice::shard).orElse(ProcessingResult.UNKNOWN_SHARD));
        }
        String shard;
        if (outcome == ProcessingOutcome.STALE) {
            shard = step("update devices: stale event")
                    .observe(() -> repository.countStale(change.deviceId(), change.eventAt()));
        } else if (stored.isEmpty()) {
            shard = step("insert devices").observe(() -> repository.insert(toRecord(change, null)));
        } else if (change.hasState()) {
            shard = step("update devices: snapshot")
                    .observe(() -> repository.updateWithState(toRecord(change, stored.get())));
        } else {
            shard = step("update devices: event")
                    .observe(() -> repository.updateWithoutState(toRecord(change, stored.get())));
        }
        return new ProcessingResult(outcome, shard);
    }

    /**
     * APPLIED или STALE. Состояние (снимок, удаление) сравнивается со временем состояния строки, а не с последним
     * событием: у сообщений 1.0.0 время - момент публикации, а событие без снимка состояния не несёт.
     * Равное время - APPLIED: повторную публикацию того же события отсекает журнал, а не время.
     */
    static ProcessingOutcome outcomeOf(DeviceChange change, StoredDevice stored) {
        if (stored == null) {
            return ProcessingOutcome.APPLIED;
        }
        if (change.changesState()) {
            return stored.lastStateAt() == null || !change.eventAt().isBefore(stored.lastStateAt())
                    ? ProcessingOutcome.APPLIED
                    : ProcessingOutcome.STALE;
        }
        return change.eventAt().isBefore(stored.lastEventAt()) ? ProcessingOutcome.STALE : ProcessingOutcome.APPLIED;
    }

    /** Новое состояние строки devices; stored - текущая строка или null для нового устройства. */
    static DeviceRecord toRecord(DeviceChange change, StoredDevice stored) {
        DeviceState state = change.state();
        String status;
        Instant deletedAt;
        if (change.deletes()) {
            status = DeviceState.DELETED;
            deletedAt = change.eventAt();
        } else if (state != null) {
            // Более новое событие со снимком после удаления возвращает устройство
            status = state.status();
            deletedAt = null;
        } else if (stored != null) {
            status = stored.status();
            deletedAt = stored.deletedAt();
        } else {
            status = DeviceState.UNKNOWN;
            deletedAt = null;
        }
        // Последняя известная активность: время события, время из снимка и уже сохранённое - что позже
        Instant lastSeenAt = Stream.of(change.eventAt(), state == null ? null : state.lastSeenAt(),
                        stored == null ? null : stored.lastSeenAt())
                .filter(Objects::nonNull)
                .max(Instant::compareTo)
                .orElse(change.eventAt());
        return new DeviceRecord(change.deviceId(), state, status, lastSeenAt, deletedAt, change.eventAt(),
                change.eventId(), change.type(), change.changesState() ? change.eventAt() : null);
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