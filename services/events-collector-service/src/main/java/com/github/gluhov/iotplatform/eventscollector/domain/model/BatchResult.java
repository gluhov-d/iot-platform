package com.github.gluhov.iotplatform.eventscollector.domain.model;

public record BatchResult(boolean leaseAcquired, int selected, int published, int skipped, int failed, int errors) {

    public static BatchResult leaseNotAcquired() {
        return new BatchResult(false,0,0,0,0,0);
    }
}
