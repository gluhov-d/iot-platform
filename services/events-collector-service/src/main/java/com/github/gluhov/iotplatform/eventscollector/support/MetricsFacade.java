package com.github.gluhov.iotplatform.eventscollector.support;

import com.github.gluhov.iotplatform.eventscollector.domain.port.out.CollectorMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

// Имена без суффикса total: Prometheus-реестр добавит его сам (events.processed -> events_processed_total).
// outbox.candidate.created экспортируется как outbox_candidate_total: Prometheus Java client 1.x
// запрещает суффикс _created, требуемое имя восстанавливает metric_relabel_configs в prometheus.yaml.
@Component
public class MetricsFacade implements CollectorMetrics {
    private final Counter eventsProcessed;
    private final Counter eventsPersistFail;
    private final Counter outboxCandidateCreated;
    private final Counter publishSuccess;
    private final Counter publishFail;
    private final Counter publishSkipped;
    private final AtomicLong pending = new AtomicLong();

    public MetricsFacade(MeterRegistry registry) {
        eventsProcessed = counter(registry, "events.processed", "Successfully processed incoming events");
        eventsPersistFail = counter(registry, "events.persist.fail", "ClickHouse write errors");
        outboxCandidateCreated = counter(registry, "outbox.candidate.created", "Created outbox records");
        publishSuccess = counter(registry, "outbox.publish.success", "Successful publications to devices");
        publishFail = counter(registry, "outbox.publish.fail", "Failed publications to devices");
        publishSkipped = counter(registry, "outbox.publish.skipped", "Skipped publications: deviceId already published");
        Gauge.builder("outbox.pending.count", pending, AtomicLong::get)
                .description("Outbox records NEW/FAILED waiting for processing")
                .register(registry);
    }

    @Override
    public void eventProcessed() {
        eventsProcessed.increment();
    }

    @Override
    public void eventPersistFailed() {
        eventsPersistFail.increment();
    }

    @Override
    public void outboxCandidateCreated() {
        outboxCandidateCreated.increment();
    }

    @Override
    public void publishSucceeded() {
        publishSuccess.increment();
    }

    @Override
    public void publishFailed() {
        publishFail.increment();
    }

    @Override
    public void publishSkipped() {
        publishSkipped.increment();
    }

    @Override
    public void pendingCount(long value) {
        pending.set(value);
    }

    private static Counter counter(MeterRegistry registry, String name, String description) {
        return Counter.builder(name).description(description).register(registry);
    }
}
