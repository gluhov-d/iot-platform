package com.github.gluhov.iotplatform.eventscollector.service.domain.service;

import com.github.gluhov.iotplatform.eventscollector.domain.port.out.PublisherLease;
import com.github.gluhov.iotplatform.eventscollector.domain.service.LeaseGuard;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

// Фоновое продление проверяется на настоящем планировщике с коротким TTL (период heartbeat - TTL/3 = 100 мс)
@ExtendWith(MockitoExtension.class)
class LeaseGuardTest {
    private static final String TOKEN = "owner-token";
    private static final Duration SHORT_TTL = Duration.ofMillis(300);
    private static final Duration LONG_TTL = Duration.ofSeconds(30);

    @Mock
    private PublisherLease lease;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void tearDown() {
        scheduledExecutorService.shutdownNow();
    }

    @Test
    @DisplayName("Lease held then heartbeat extends it in background")
    void givenHeldLease_whenTimePasses_thenHeartbeatExtends() {
        when(lease.extend(TOKEN, SHORT_TTL)).thenReturn(true);

        try (LeaseGuard guard = new LeaseGuard(lease, TOKEN, SHORT_TTL, scheduledExecutorService)) {
            verify(lease, timeout(1_000).atLeast(3)).extend(TOKEN, SHORT_TTL);
            assertThat(guard.isLost()).isFalse();
            assertThat(guard.renew()).isTrue();
        }
    }

    @Test
    @DisplayName("Lease taken by another instance then heartbeat marks it lost and stops extending")
    void givenForeignLease_whenHeartbeat_thenLostAndNoMoreExtends() {
        when(lease.extend(TOKEN, SHORT_TTL)).thenReturn(false);

        try (LeaseGuard guard = new LeaseGuard(lease, TOKEN, SHORT_TTL, scheduledExecutorService)) {
            await().atMost(Duration.ofSeconds(1)).until(guard::isLost);
            assertThat(guard.renew()).as("lost lease is not regained").isFalse();
            verify(lease, after(500).times(1)).extend(TOKEN, SHORT_TTL);
        }
    }

    @Test
    @DisplayName("Redis fails on extend then lease considered lost")
    void givenRedisFailure_whenRenew_thenLost() {
        when(lease.extend(TOKEN, LONG_TTL)).thenThrow(new IllegalStateException("redis down"));

        try (LeaseGuard guard = new LeaseGuard(lease, TOKEN, LONG_TTL, scheduledExecutorService)) {
            assertThat(guard.renew()).isFalse();
            assertThat(guard.isLost()).isTrue();
        }
    }

    @Test
    @DisplayName("Guard closed then heartbeat stops")
    void givenClosedGuard_whenTimePasses_thenNoExtends() {
        new LeaseGuard(lease, TOKEN, SHORT_TTL, scheduledExecutorService).close();

        verify(lease, after(500).never()).extend(anyString(), any());
    }
}
