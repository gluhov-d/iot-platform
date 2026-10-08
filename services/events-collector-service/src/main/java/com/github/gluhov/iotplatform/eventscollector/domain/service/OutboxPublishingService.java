package com.github.gluhov.iotplatform.eventscollector.domain.service;

import com.github.gluhov.iotplatform.eventscollector.domain.model.BatchResult;
import com.github.gluhov.iotplatform.eventscollector.domain.model.OutboxPolicy;
import com.github.gluhov.iotplatform.eventscollector.domain.model.OutboxRecord;
import com.github.gluhov.iotplatform.eventscollector.domain.model.PublishOutcome;
import com.github.gluhov.iotplatform.eventscollector.domain.port.in.PublishOutboxUseCase;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.*;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

import java.time.Clock;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class OutboxPublishingService implements PublishOutboxUseCase {
    private final PublisherLease lease;
    private final OutboxRepository outboxRepository;
    private final PublishedDeviceStore publishedDeviceStore;
    private final DeviceInfoPublisher publisher;
    private final CollectorMetrics metrics;
    private final ObservationRegistry observationRegistry;
    private final OutboxPolicy policy;
    private final Clock clock;

    @Override
    public BatchResult publishPendingBatch() {
        Optional<String> ownerToken = lease.tryAcquire(policy.leaseTtl());
        if (ownerToken.isEmpty()) {
            log.debug("Outbox lease is held by another instance, iteration skipped");
            refreshPendingCount();
            return BatchResult.leaseNotAcquired();
        }
        try {
            return Observation.createNotStarted("outbox.batch", observationRegistry)
                    .contextualName("outbox batch")
                    .observe(this::publishBatch);
        } finally {
            if (!lease.release(ownerToken.get())) {
                log.warn("Outbox lease expired before the iteration finished, increase OUTBOX_LEASE_TTL_MS");
            }
            refreshPendingCount();
        }
    }

    private BatchResult publishBatch() {
        List<OutboxRecord> pending = outboxRepository.findPending(policy.maxAttempts(), policy.batchSize());
        Map<PublishOutcome, Integer> outcomes = new EnumMap<>(PublishOutcome.class);
        for (OutboxRecord record: pending) {
            outcomes.merge(processOne(record), 1, Integer::sum);
        }
        BatchResult result = new BatchResult(true, pending.size(),
                outcomes.getOrDefault(PublishOutcome.PUBLISHED, 0),
                outcomes.getOrDefault(PublishOutcome.SKIPPED, 0),
                outcomes.getOrDefault(PublishOutcome.FAILED, 0),
                outcomes.getOrDefault(PublishOutcome.ERROR, 0));
        if (!pending.isEmpty()) {
            log.info("Outbox batch completed: {}", result);
        }
        return result;
    }

    PublishOutcome processOne(OutboxRecord record) {
        try (var ignoredEvent = MDC.putCloseable("eventId", record.eventId());
             var ignoredDevice = MDC.putCloseable("deviceId", record.deviceId())) {
            return Observation.createNotStarted("outbox.publish", observationRegistry)
                    .contextualName("outbox publish")
                    .highCardinalityKeyValue("eventId", record.eventId())
                    .highCardinalityKeyValue("deviceId", record.deviceId())
                    .observe(() -> publishOrSkip(record));
        } catch (RuntimeException e) {
            // Не удалось записать новую версию: запись останется NEW/FAILED и будет выбрана снова
            logOutcome(PublishOutcome.ERROR, e);
            return PublishOutcome.ERROR;
        }
    }

    private PublishOutcome publishOrSkip(OutboxRecord record) {
        boolean alreadyPublished = step("redis devices:published check")
                .observe(() -> publishedDeviceStore.isPublished(record.deviceId()));
        if (alreadyPublished) {
            step("append SENT version").observe(() -> outboxRepository.insert(record.sent(now())));
            metrics.publishSkipped();
            logOutcome(PublishOutcome.SKIPPED, null);
            return PublishOutcome.SKIPPED;
        }

        try {
            step("publish devices").observe(() -> publisher.publish(record.deviceId(), now().toEpochMilli()));
        } catch (RuntimeException e) {
            outboxRepository.insert(record.failed(now(), describe(e)));
            metrics.publishFailed();
            logOutcome(PublishOutcome.FAILED, e);
            return PublishOutcome.FAILED;
        }

        // Только после подтверждения Kafka: иначе устройство могло бы остаться неопубликованным навсегда
        step("redis devices:published add").observe(() -> publishedDeviceStore.markPublished(record.deviceId()));
        step("append SENT version").observe(() -> outboxRepository.insert(record.sent(now())));
        metrics.publishSucceeded();
        logOutcome(PublishOutcome.PUBLISHED, null);
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
}
