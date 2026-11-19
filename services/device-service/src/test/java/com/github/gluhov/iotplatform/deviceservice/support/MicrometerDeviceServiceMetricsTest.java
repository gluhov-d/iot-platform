package com.github.gluhov.iotplatform.deviceservice.support;

import com.github.gluhov.iotplatform.deviceservice.domain.model.CacheResult;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceCommand.Operation;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerDeviceServiceMetricsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final MicrometerDeviceServiceMetrics metrics = new MicrometerDeviceServiceMetrics(registry);

    @Test
    void givenPrecheckFromCache_whenCounted_thenCounterByOperationAndResult() {
        metrics.commandPrecheckFromCache(Operation.UPDATE, CacheResult.HIT);
        metrics.commandPrecheckFromCache(Operation.UPDATE, CacheResult.HIT);
        metrics.commandPrecheckFromCache(Operation.DELETE, CacheResult.MISS);

        assertThat(registry.get("device.command.precheck.cache").tag("operation", "update").tag("result", "hit")
                .counter().count()).isEqualTo(2.0);
        assertThat(registry.get("device.command.precheck.cache").tag("operation", "delete").tag("result", "miss")
                .counter().count()).isEqualTo(1.0);
    }

    @Test
    void givenNoCalls_whenRegistered_thenCountersExistFromZero() {
        assertThat(registry.get("device.command.precheck.cache").tag("operation", "create").tag("result", "error")
                .counter().count()).isZero();
    }
}
