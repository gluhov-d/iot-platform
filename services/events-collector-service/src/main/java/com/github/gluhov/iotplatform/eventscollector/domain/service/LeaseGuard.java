package com.github.gluhov.iotplatform.eventscollector.domain.service;

import com.github.gluhov.iotplatform.eventscollector.domain.port.out.PublisherLease;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

// Удерживает аренду публикатора на время итерации: продлевает ее в фоне каждые ttl/3, чтобы она не истекла
// во время долгих блокирующих вызовов (выборка, отправка в Kafka, запись в ClickHouse), и по запросу в
// контрольных точках. Не удалось продлить или Redis недоступен - аренда считается потерянной навсегда.
@Slf4j
public final class LeaseGuard implements AutoCloseable {
    private final PublisherLease lease;
    private final String ownerToken;
    private final Duration ttl;
    private final AtomicBoolean lost = new AtomicBoolean();
    private volatile boolean closed;
    private final ScheduledFuture<?> heartbeat;

    public LeaseGuard(PublisherLease lease, String ownerToken, Duration ttl, ScheduledExecutorService scheduler) {
        this.lease = lease;
        this.ownerToken = ownerToken;
        this.ttl = ttl;
        long periodMs = Math.max(1L, ttl.toMillis() / 3);
        this.heartbeat = scheduler.scheduleWithFixedDelay(this::renew, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    // Продлить аренду сейчас; false - аренда потеряна, работу нужно остановить
    public boolean renew() {
        if (lost.get()) {
            return false;
        }
        try {
            if (!lease.extend(ownerToken, ttl)) {
                markLost("lease expired or taken by another instance");
            }
        } catch (RuntimeException e) {
            // Владение нельзя подтвердить - продолжать так же опасно, как с чужой арендой
            markLost(e.toString());
        }
        return !lost.get();
    }

    public boolean isLost() {
        return lost.get();
    }

    // Остановить фоновое продление (саму аренду освобождает вызывающий код)
    @Override
    public void close() {
        closed = true;
        heartbeat.cancel(false);
    }

    private void markLost(String reason) {
        // После close() аренду освобождают намеренно: продление, начатое до отмены, потерей не считается
        if (!closed && lost.compareAndSet(false, true)) {
            log.warn("Outbox lease lost: {}", reason);
        }
    }
}
