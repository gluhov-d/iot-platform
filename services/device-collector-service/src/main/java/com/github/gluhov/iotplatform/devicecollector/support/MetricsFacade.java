package com.github.gluhov.iotplatform.devicecollector.support;

import com.github.gluhov.iotplatform.devicecollector.domain.model.FailureReason;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessingOutcome;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessingResult;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StorageFailureReason;
import com.github.gluhov.iotplatform.devicecollector.domain.port.out.DeviceMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;

/**
 * Метрики из задания. Имена без суффиксов: Prometheus-реестр добавит их сам
 * (devices.messages.received -> devices_messages_received_total, таймер devices.db.write -> devices_db_write_seconds).
 * Счётчики с известными метками регистрируются сразу - в /actuator/prometheus они есть с нуля
 * (devices_shard_route_fail_total - с меткой shard=unknown: имена шардов сообщает хранилище).
 * Метки шарда - только те, что сообщило хранилище (shard_0, shard_1, unknown): топологию сервис не хранит.
 */
@Component
public class MetricsFacade implements DeviceMetrics {
    private final Counter received;
    private final Counter duplicate;
    private final Map<ProcessingOutcome, Counter> processed = new EnumMap<>(ProcessingOutcome.class);
    private final Map<FailureReason, Counter> failed = new EnumMap<>(FailureReason.class);
    private final Map<FailureReason, Counter> dltPublished = new EnumMap<>(FailureReason.class);
    private final Map<FailureReason, Counter> processingErrors = new EnumMap<>(FailureReason.class);
    private final Meter.MeterProvider<Timer> processing;
    private final Meter.MeterProvider<Timer> dbWrite;
    private final Meter.MeterProvider<Counter> shardRouteFail;

    public MetricsFacade(MeterRegistry registry) {
        received = Counter.builder("devices.messages.received")
                .description("Messages received from Kafka devices; a redelivery after an error is not counted again")
                .register(registry);
        duplicate = Counter.builder("devices.messages.duplicate")
                .description("Repeated events skipped: already in processed_device_events")
                .register(registry);
        for (ProcessingOutcome outcome : new ProcessingOutcome[]{ProcessingOutcome.APPLIED, ProcessingOutcome.STALE}) {
            processed.put(outcome, Counter.builder("devices.messages.processed")
                    .description("Messages processed: applied - state updated, stale - older event, only counted")
                    .tag("outcome", outcome.label())
                    .register(registry));
        }
        for (FailureReason reason : FailureReason.values()) {
            failed.put(reason, Counter.builder("devices.messages.failed")
                    .description("Messages that ended with an error: moved to devices-dlt")
                    .tag("reason", reason.label())
                    .register(registry));
            dltPublished.put(reason, Counter.builder("devices.dlt.published")
                    .description("Messages published to devices-dlt")
                    .tag("reason", reason.label())
                    .register(registry));
            processingErrors.put(reason, Counter.builder("devices.processing.errors")
                    .description("Processing attempts that ended with an error (retried or moved to devices-dlt)")
                    .tag("reason", reason.label())
                    .register(registry));
        }
        processing = Timer.builder("devices.processing")
                .description("Full message processing time")
                .withRegistry(registry);
        dbWrite = Timer.builder("devices.db.write")
                .description("PostgreSQL transaction time through ShardingSphere-Proxy")
                .withRegistry(registry);
        shardRouteFail = Counter.builder("devices.shard.route.fail")
                .description("Routing or shard access errors")
                .withRegistry(registry);
        for (StorageFailureReason reason : StorageFailureReason.values()) {
            if (reason.isShardAccess()) {
                shardRouteFail.withTags("shard", ProcessingResult.UNKNOWN_SHARD, "reason", reason.label());
            }
        }
    }

    @Override
    public void received() {
        received.increment();
    }

    @Override
    public void processed(ProcessingOutcome outcome) {
        if (outcome == ProcessingOutcome.DUPLICATE) {
            duplicate.increment();
        } else {
            processed.get(outcome).increment();
        }
    }

    @Override
    public void failed(FailureReason reason) {
        failed.get(reason).increment();
    }

    @Override
    public void processingError(FailureReason reason) {
        processingErrors.get(reason).increment();
    }

    @Override
    public void dltPublished(FailureReason reason) {
        dltPublished.get(reason).increment();
    }

    @Override
    public void processingTime(String outcome, Duration duration) {
        processing.withTags("outcome", outcome).record(duration);
    }

    @Override
    public void dbWrite(String shard, boolean success, Duration duration) {
        dbWrite.withTags("shard", shard, "result", success ? "success" : "error").record(duration);
    }

    @Override
    public void shardRouteFailed(String shard, StorageFailureReason reason) {
        shardRouteFail.withTags("shard", shard, "reason", reason.label()).increment();
    }
}