package com.github.gluhov.iotplatform.eventscollector.domain.service;

import com.github.gluhov.iotplatform.eventscollector.domain.exception.LeaseLostException;
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
        boolean leaseLost = false;
        for (int i = 0; i < pending.size(); i++) {
            // Контрольная точка перед каждой записью, в том числе первой: выборка могла длиться дольше TTL,
            // и аренду уже получил другой экземпляр. Не продлилась - публиковать дальше нельзя
            if (!renewLease(guard)) {
                leaseLost = true;
                log.warn("Outbox lease lost, batch stopped: {} of {} records processed", i, pending.size());
                break;
            }
            try {
                outcomes.merge(processOne(pending.get(i), guard), 1, Integer::sum);
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
                leaseLost);
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

    private PublishOutcome publishOrSkip(OutboxRecord record, LeaseGuard guard) {
        boolean alreadyPublished = step("redis devices:published check")
                .observe(() -> publishedDeviceStore.isPublished(record.deviceId()));
        if (alreadyPublished) {
            ensureLease(guard);
            step("append SENT version").observe(() -> outboxRepository.insert(record.sent(now())));
            metrics.publishSkipped();
            logOutcome(PublishOutcome.SKIPPED, null);
            audit(record, PublishOutcome.SKIPPED, null);
            return PublishOutcome.SKIPPED;
        }

        try {
            step("publish devices").observe(() -> publisher.publish(record.deviceId(), now().toEpochMilli()));
        } catch (RuntimeException e) {
            ensureLease(guard);
            outboxRepository.insert(record.failed(now(), describe(e)));
            metrics.publishFailed();
            logOutcome(PublishOutcome.FAILED, e);
            audit(record, PublishOutcome.FAILED, e);
            return PublishOutcome.FAILED;
        }

        // Только после подтверждения Kafka: иначе устройство могло бы остаться неопубликованным навсегда
        step("redis devices:published add").observe(() -> publishedDeviceStore.markPublished(record.deviceId()));
        ensureLease(guard);
        step("append SENT version").observe(() -> outboxRepository.insert(record.sent(now())));
        metrics.publishSucceeded();
        logOutcome(PublishOutcome.PUBLISHED, null);
        audit(record, PublishOutcome.PUBLISHED, null);
        return PublishOutcome.PUBLISHED;
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
