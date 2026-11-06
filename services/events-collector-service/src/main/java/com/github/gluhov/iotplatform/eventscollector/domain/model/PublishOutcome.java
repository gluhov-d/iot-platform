package com.github.gluhov.iotplatform.eventscollector.domain.model;

public enum PublishOutcome {
    PUBLISHED,
    SKIPPED,
    FAILED,
    ERROR,
    /** Kafka или Schema Registry временно недоступны: попытка не засчитана, пачка остановлена. Не пишется в аудит. */
    DEFERRED
}