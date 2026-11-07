package com.github.gluhov.iotplatform.eventscollector.domain.service;

import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceSnapshot;
import com.github.gluhov.iotplatform.events.avro.DeviceStatus;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.LeaseLostException;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.PublisherUnavailableException;
import com.github.gluhov.iotplatform.eventscollector.domain.model.*;
import com.github.gluhov.iotplatform.eventscollector.domain.port.in.PublishOutboxUseCase;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.*;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.OptionalLong;
import java.util.Set;
import java.util.Map;
import java.util.Optional;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Service
@Slf4j
@RequiredArgsConstructor
public class OutboxPublishingService implements PublishOutboxUseCase {
    private final PublisherLease lease;
    private final OutboxRepository outboxRepository;
    private final PublishedDeviceStore publishedDeviceStore;
    private final DeviceInfoPublisher publisher;
    private final PublicationAuditRepository publicationAuditRepository;
    private final CollectorMetrics metrics;
    private final ObservationRegistry observationRegistry;
    private final TraceContextCarrier traceContextCarrier;
    private final OutboxPolicy policy;
    private final Clock clock;
    // Поток фонового продления аренды (LeaseGuard); демон - не мешает остановке приложения.
    // Не бин Spring: бин ScheduledExecutorService отключил бы планировщик Spring Boot для @Scheduled
    private final ScheduledExecutorService leaseHeartbeat = Executors.newSingleThreadScheduledExecutor(
            Thread.ofPlatform().name("outbox-lease-heartbeat").daemon().factory());

    @Override
    public BatchResult publishPendingBatch() {
        Optional<String> ownerToken = lease.tryAcquire(policy.leaseTtl());
        if (ownerToken.isEmpty()) {
            log.debug("Outbox lease is held by another instance, iteration skipped");
            refreshPendingCount();
            return BatchResult.leaseNotAcquired();
        }
        // Аренда удерживается с момента захвата: фоновое продление идет и во время выборки
        LeaseGuard guard = new LeaseGuard(lease, ownerToken.get(), policy.leaseTtl(), leaseHeartbeat);
        try {
            return Observation.createNotStarted("outbox.batch", observationRegistry)
                    .contextualName("outbox batch")
                    .observe(() -> publishBatch(guard));
        } finally {
            guard.close();
            // Потерю аренды guard уже записал в лог; здесь - истечение TTL без потери владения в контрольных точках
            if (!lease.release(ownerToken.get()) && !guard.isLost()) {
                log.warn("Outbox lease expired before the iteration finished, increase OUTBOX_LEASE_TTL_MS");
            }
            refreshPendingCount();
        }
    }

    private BatchResult publishBatch(LeaseGuard guard) {
        List<OutboxRecord> pending = outboxRepository.findPending(policy.maxAttempts(), policy.batchSize());
        Map<PublishOutcome, Integer> outcomes = new EnumMap<>(PublishOutcome.class);
        // Устройства, у которых в этой пачке запись не опубликована: их следующие записи ждут следующей итерации,
        // иначе более позднее событие ушло бы раньше (CREATED/UPDATED перепутались бы, в compacted-теме остался бы
        // устаревший снимок)
        Set<String> blockedDevices = new HashSet<>();
        boolean leaseLost = false;
        for (int i = 0; i < pending.size(); i++) {
            OutboxRecord record = pending.get(i);
            if (blockedDevices.contains(record.deviceId())) {
                outcomes.merge(PublishOutcome.DEFERRED, 1, Integer::sum);
                continue;
            }
            // Контрольная точка перед каждой записью, в том числе первой: выборка могла длиться дольше TTL,
            // и аренду уже получил другой экземпляр. Не продлилась - публиковать дальше нельзя
            if (!renewLease(guard)) {
                leaseLost = true;
                log.warn("Outbox lease lost, batch stopped: {} of {} records processed", i, pending.size());
                break;
            }
            try {
                PublishOutcome outcome = processOne(record, guard);
                outcomes.merge(outcome, 1, Integer::sum);
                if (outcome == PublishOutcome.DEFERRED) {
                    // Kafka/Schema Registry недоступны: остальные записи пачки упали бы так же
                    outcomes.merge(PublishOutcome.DEFERRED, pending.size() - i - 1, Integer::sum);
                    log.warn("DeviceInfo publisher unavailable, batch stopped: {} of {} records processed",
                            i, pending.size());
                    break;
                }
                if (outcome == PublishOutcome.FAILED || outcome == PublishOutcome.ERROR) {
                    blockedDevices.add(record.deviceId());
                }
            } catch (LeaseLostException e) {
                // Аренда потеряна во время обработки записи: новая версия outbox не записана
                leaseLost = true;
                log.warn("Outbox lease lost while processing record, batch stopped: {} of {} records processed",
                        i, pending.size());
                break;
            }
        }
        BatchResult result = new BatchResult(true, pending.size(),
                outcomes.getOrDefault(PublishOutcome.PUBLISHED, 0),
                outcomes.getOrDefault(PublishOutcome.SKIPPED, 0),
                outcomes.getOrDefault(PublishOutcome.FAILED, 0),
                outcomes.getOrDefault(PublishOutcome.ERROR, 0),
                leaseLost,
                outcomes.getOrDefault(PublishOutcome.DEFERRED, 0));
        if (!pending.isEmpty()) {
            log.info("Outbox batch completed: {}", result);
        }
        return result;
    }

    PublishOutcome processOne(OutboxRecord record, LeaseGuard guard) {
        // Без catch: ресурсы try-with-resources закрываются до catch, и ошибка записалась бы уже без
        // eventId/deviceId в MDC и вне трейса приема. Ошибку обрабатывает publishInEventTrace внутри этих областей
        try (var ignoredEvent = MDC.putCloseable("eventId", record.eventId());
             var ignoredDevice = MDC.putCloseable("deviceId", record.deviceId())) {
            return traceContextCarrier.continueTrace(record.traceRef(), () -> publishInEventTrace(record, guard));
        }
    }

    // Публикация продолжает трейс приема: родитель спана - спан приема из outbox, поэтому
    // родительское наблюдение пачки сбрасывается (иначе спан записи стал бы ребенком пачки)
    private PublishOutcome publishInEventTrace(OutboxRecord record, LeaseGuard guard) {
        Observation observation = Observation.createNotStarted("outbox.publish", observationRegistry)
                .parentObservation(null)
                .contextualName("outbox publish")
                .highCardinalityKeyValue("eventId", record.eventId())
                .highCardinalityKeyValue("deviceId", record.deviceId());
        return observation.observe(() -> {
            try {
                return publishOrSkip(record, guard);
            } catch (LeaseLostException e) {
                // Не ошибка записи, а потеря права публиковать: решает publishBatch
                throw e;
            } catch (RuntimeException e) {
                // Не удалось записать новую версию: запись останется NEW/FAILED и будет выбрана снова.
                // Спан outbox publish еще открыт: ошибка отмечается в нем, журнал и аудит - в трейсе приема
                observation.error(e);
                logOutcome(PublishOutcome.ERROR, e);
                audit(record, PublishOutcome.ERROR, e);
                return PublishOutcome.ERROR;
            }
        });
    }

    // Технический аудит пишется после смены статуса в outbox; его ошибка не меняет результат публикации
    private void audit(OutboxRecord record, PublishOutcome publishOutcome, Throwable error) {
        PublicationAuditRecord entry = PublicationAuditRecord.of(record, publishOutcome, error == null ? null : describe(error), now());
        try {
            step("append audit record").observe(() -> publicationAuditRepository.insert(entry));
        } catch (RuntimeException e) {
            log.warn("Publication audit record not written: {}", describe(e));
        }
    }

    // Правило публикации (реестр устройств):
    //   устройства нет в devices:published          -> CREATED (со снимком, если он есть);
    //   устройство есть, у события есть снимок      -> UPDATED;
    //   снимок со статусом DELETED                   -> DELETED (в том числе для нового устройства);
    //   устройство есть, снимка нет (телеметрия)    -> SKIPPED: поток devices не раздувается;
    //   снимок старше уже опубликованного снимка     -> SKIPPED: в compacted-теме devices последним по ключу
    //                                                   остаётся самое позднее состояние устройства.
    private PublishOutcome publishOrSkip(OutboxRecord record, LeaseGuard guard) {
        boolean alreadyPublished = step("redis devices:published check")
                .observe(() -> publishedDeviceStore.isPublished(record.deviceId()));
        if (alreadyPublished && !record.hasSnapshot()) {
            return skip(record, guard, "no device snapshot, device already published");
        }
        OptionalLong lastSnapshotAt = record.hasSnapshot() && alreadyPublished
                ? step("redis devices:snapshot-at get").observe(() -> publishedDeviceStore.lastSnapshotAt(record.deviceId()))
                : OptionalLong.empty();
        if (lastSnapshotAt.isPresent() && record.sourceTimestampMs() < lastSnapshotAt.getAsLong()) {
            return skip(record, guard, "snapshot is older than the published one");
        }

        DeviceInfo deviceInfo;
        try {
            deviceInfo = toDeviceInfo(record, alreadyPublished);
        } catch (IllegalArgumentException e) {
            // Снимок не читается: дефект записи, повтор не поможет - FAILED с attempts + 1, а не ERROR каждые 2 с
            return failed(record, guard, e);
        }
        // Тип изменения - в MDC на всё время публикации, включая журнал ошибки отправки
        try (var ignored = MDC.putCloseable("eventType", deviceInfo.getEventType().name())) {
            return publish(record, deviceInfo, guard, lastSnapshotAt);
        }
    }

    private PublishOutcome skip(OutboxRecord record, LeaseGuard guard, String reason) {
        ensureLease(guard);
        step("append SENT version").observe(() -> outboxRepository.insert(record.sent(now())));
        metrics.publishSkipped();
        log.debug("Outbox record skipped: {}", reason);
        logOutcome(PublishOutcome.SKIPPED, null);
        audit(record, PublishOutcome.SKIPPED, null);
        return PublishOutcome.SKIPPED;
    }

    private PublishOutcome failed(OutboxRecord record, LeaseGuard guard, RuntimeException e) {
        ensureLease(guard);
        outboxRepository.insert(record.failed(now(), describe(e)));
        metrics.publishFailed();
        logOutcome(PublishOutcome.FAILED, e);
        audit(record, PublishOutcome.FAILED, e);
        return PublishOutcome.FAILED;
    }

    private PublishOutcome publish(OutboxRecord record, DeviceInfo deviceInfo, LeaseGuard guard, OptionalLong lastSnapshotAt) {
        try {
            step("publish devices").observe(() -> publisher.publish(deviceInfo));
        } catch (PublisherUnavailableException e) {
            // Недоступна Kafka или Schema Registry: запись не меняется (attempts тот же), аудит не пишется
            metrics.publishFailed();
            logOutcome(PublishOutcome.DEFERRED, e);
            return PublishOutcome.DEFERRED;
        } catch (RuntimeException e) {
            return failed(record, guard, e);
        }

        // Только после подтверждения Kafka: иначе устройство могло бы остаться неопубликованным навсегда
        step("redis devices:published add").observe(() -> publishedDeviceStore.markPublished(record.deviceId()));
        if (record.hasSnapshot()) {
            long snapshotAt = Math.max(record.sourceTimestampMs(), lastSnapshotAt.orElse(Long.MIN_VALUE));
            step("redis devices:snapshot-at set").observe(() -> publishedDeviceStore.markSnapshotAt(record.deviceId(), snapshotAt));
        }
        ensureLease(guard);
        step("append SENT version").observe(() -> outboxRepository.insert(record.sent(now())));
        metrics.publishSucceeded();
        logOutcome(PublishOutcome.PUBLISHED, null);
        audit(record, PublishOutcome.PUBLISHED, null);
        return PublishOutcome.PUBLISHED;
    }

    // eventId - ключ идемпотентности потребителя: одинаков при повторной публикации той же записи outbox,
    // а emittedAtMs - время этой попытки. eventTimeMs - время исходного события: по нему отсекаются устаревшие
    private DeviceInfo toDeviceInfo(OutboxRecord record, boolean alreadyPublished) {
        DeviceSnapshot snapshot = DeviceSnapshotJson.fromJson(record.deviceSnapshot(), record.deviceSnapshotSchema());
        DeviceUpsertEventType eventType;
        if (snapshot != null && snapshot.getStatus() == DeviceStatus.DELETED) {
            eventType = DeviceUpsertEventType.DELETED;
        } else {
            eventType = alreadyPublished ? DeviceUpsertEventType.UPDATED : DeviceUpsertEventType.CREATED;
        }
        return DeviceInfo.newBuilder()
                .setDeviceId(record.deviceId())
                .setEmittedAtMs(now().toEpochMilli())
                .setEventType(eventType)
                .setEventId(record.eventId())
                .setEventTimeMs(record.sourceTimestampMs())
                .setDevice(snapshot)
                .build();
    }

    private void refreshPendingCount() {
        try {
            metrics.pendingCount(outboxRepository.countPending(policy.maxAttempts()));
        } catch (RuntimeException e) {
            log.warn("Failed to refresh outbox_pending_count: {}", e.toString());
        }
    }

    private Observation step(String name) {
        return Observation.createNotStarted("outbox.step", observationRegistry)
                .contextualName(name)
                .lowCardinalityKeyValue("step", name);
    }

    private Instant now() {
        return clock.instant();
    }

    private static String describe(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }

    private static void logOutcome(PublishOutcome outcome, Throwable error) {
        try (var ignored = MDC.putCloseable("status", outcome.name())) {
            if (error == null) {
                log.info("Outbox record processed: {}", outcome);
                return;
            }

            try (var c = MDC.putCloseable("errorClass", error.getClass().getName());
                 var m = MDC.putCloseable("errorMessage", String.valueOf(error.getMessage()))) {
                if (outcome == PublishOutcome.ERROR) {
                    log.error("Outbox record not processed, will be selected again", error);
                } else {
                    log.warn("DeviceInfo publish failed: {}", describe(error));
                }
            }
        }
    }

    // Контрольная точка: продлить аренду сейчас (синхронно, в спане шага)
    private boolean renewLease(LeaseGuard guard) {
        return step("redis outbox:lease extend").observe(guard::renew);
    }

    // Перед записью новой версии outbox после внешних вызовов: аренда должна быть нашей, иначе два публикатора
    // могли бы записать разные состояния с одинаковым state_version
    private void ensureLease(LeaseGuard guard) {
        if (!renewLease(guard)) {
            throw new LeaseLostException("Outbox lease lost before writing a new outbox version");
        }
    }
}