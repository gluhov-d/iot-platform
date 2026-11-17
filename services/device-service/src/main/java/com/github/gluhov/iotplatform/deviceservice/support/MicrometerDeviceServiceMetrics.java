package com.github.gluhov.iotplatform.deviceservice.support;

import com.github.gluhov.iotplatform.deviceservice.domain.model.CacheResult;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceCommand;
import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceServiceMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;

/**
 * device_cache_requests_total{result=hit|miss|stale_replica|error}, device_commands_accepted_total{operation},
 * device_commands_rejected_total{operation,code}, device_command_publish_seconds{operation}: Prometheus добавит суффиксы.
 * Исходы чтения регистрируются сразу - в /actuator/prometheus они видны с нуля.
 */
@Component
public class MicrometerDeviceServiceMetrics implements DeviceServiceMetrics {

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
    public void commandPublished(DeviceCommand.Operation operation, Duration duration) {
        registry.timer("device.command.publish", "operation", label(operation)).record(duration);
    }

    private static String label(DeviceCommand.Operation operation) {
        return operation.name().toLowerCase(Locale.ROOT);
    }
}
