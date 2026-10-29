package com.github.gluhov.iotplatform.eventscollector.adapter.in.scheduler;

import com.github.gluhov.iotplatform.eventscollector.domain.port.in.PublishOutboxUseCase;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class OutboxPublisherJob {
    private final PublishOutboxUseCase publishOutbox;

    // fixedDelay: следующая итерация начинается только после завершения предыдущей
    @Scheduled(
            fixedDelayString = "${app.outbox.publish-interval-ms}",
            initialDelayString = "${app.outbox.publish-interval-ms}")
    public void run() {
        try {
            publishOutbox.publishPendingBatch();
        } catch (RuntimeException e) {
            log.error("Outbox publish iteration failed", e);
        }
    }
}
