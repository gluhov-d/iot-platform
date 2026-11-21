package com.github.gluhov.iotplatform.eventscollector.domain.port.out;

public interface CollectorMetrics {

    void eventProcessed();

    void eventPersistFailed();

    void outboxCandidateCreated();

    void publishSucceeded();

    void publishFailed();

    void publishSkipped();

    void pendingCount(long value);

    /** Сообщение записано в failed-events; коды - строки ErrorCode и ErrorType. */
    void failedEventPublished(String errorCode, String errorType);
}
