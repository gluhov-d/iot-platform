package com.github.gluhov.iotplatform.deviceservice.support;

import com.github.gluhov.iotplatform.deviceservice.domain.model.CacheResult;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceCommand;
import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceServiceMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * device_cache_requests_total{result=hit|miss|stale_replica|error}, device_commands_accepted_total{operation},
 * device_commands_rejected_total{operation,code}, device_command_publish_seconds{operation},
 * device_command_precheck_cache_total{operation,result=hit|miss|error} (предварительная проверка по кэшу при
 * недоступной реплике): Prometheus добавит суффиксы.
 * Исходы чтения регистрируются сразу - в /actuator/prometheus они видны с нуля.
 */
@Component
public class MicrometerDeviceServiceMetrics implements DeviceServiceMetrics {

    private static final List<CacheResult> PRECHECK_RESULTS = List.of(CacheResult.HIT, CacheResult.MISS, CacheResult.ERROR);

    private final MeterRegistry registry;
    private final Map<CacheResult, Counter> cacheRequests = new EnumMap<>(CacheResult.class);

    public MicrometerDeviceServiceMetrics(MeterRegistry registry) {
        this.registry = registry;
        for (CacheResult result : CacheResult.values()) {
            cacheRequests.put(result, Counter.builder("device.cache.requests")
                    .description("Device reads by cache outcome: hit, miss, stale_replica (replica below floor, not cached), error (cache unavailable)")
                    .tag("result", result.label())
                    .register(registry));
        }
        for (DeviceCommand.Operation operation : DeviceCommand.Operation.values()) {
            registry.counter("device.commands.accepted", "operation", label(operation));
            for (CacheResult result : PRECHECK_RESULTS) {
                precheckCounter(operation, result);
            }
        }
    }

    @Override
    public void cacheResult(CacheResult result) {
        cacheRequests.get(result).increment();
    }

    @Override
    public void commandAccepted(DeviceCommand.Operation operation) {
        registry.counter("device.commands.accepted", "operation", label(operation)).increment();
    }

    @Override
    public void commandRejected(DeviceCommand.Operation operation, String code) {
        registry.counter("device.commands.rejected", "operation", label(operation), "code", code).increment();
    }

    @Override
    public void commandPrecheckFromCache(DeviceCommand.Operation operation, CacheResult result) {
        precheckCounter(operation, result).increment();
    }

    @Override
    public void commandPublished(DeviceCommand.Operation operation, Duration duration) {
        registry.timer("device.command.publish", "operation", label(operation)).record(duration);
    }

    private Counter precheckCounter(DeviceCommand.Operation operation, CacheResult result) {
        return Counter.builder("device.command.precheck.cache")
                .description("Command prechecks answered from the cache because the replica is unavailable: hit, miss, error (cache unavailable)")
                .tag("operation", label(operation))
                .tag("result", result.label())
                .register(registry);
    }

    private static String label(DeviceCommand.Operation operation) {
        return operation.name().toLowerCase(Locale.ROOT);
    }
}
