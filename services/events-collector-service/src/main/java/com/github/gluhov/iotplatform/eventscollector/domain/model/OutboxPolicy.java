package com.github.gluhov.iotplatform.eventscollector.domain.model;

import java.time.Duration;

public record OutboxPolicy (int batchSize, int maxAttempts, Duration leaseTtl) {

    public OutboxPolicy {
        if (batchSize <= 0 || maxAttempts <= 0 || leaseTtl.isNegative() || leaseTtl.isZero()) {
            throw new IllegalArgumentException("batchSize, maxAttempts и leaseTtl должны быть положительными");
        }
    }
}
