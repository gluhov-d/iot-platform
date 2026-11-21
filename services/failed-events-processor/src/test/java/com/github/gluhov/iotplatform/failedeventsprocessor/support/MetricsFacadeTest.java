package com.github.gluhov.iotplatform.failedeventsprocessor.support;

import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.PutOutcome;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MetricsFacadeTest {

    @Test
    void givenCreatedOutcome_whenArchived_thenCounterWithSourceErrorCodeAndOutcomeTags() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricsFacade metrics = new MetricsFacade(registry);

        metrics.archived("device-collector-service", "STORAGE_UNAVAILABLE", PutOutcome.CREATED);

        assertThat(registry.get("failed.events.archived")
                .tags("source_service", "device-collector-service",
                        "error_code", "STORAGE_UNAVAILABLE", "outcome", "created")
                .counter().count()).isEqualTo(1.0);
    }

    @Test
    void givenDuplicateOutcome_whenArchived_thenCountedAsDuplicateNotCreated() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricsFacade metrics = new MetricsFacade(registry);

        metrics.archived("events-collector-service", "VALIDATION_FAILED", PutOutcome.DUPLICATE);

        assertThat(registry.get("failed.events.archived")
                .tags("outcome", "duplicate").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("failed.events.archived").tags("outcome", "created").counter()).isNull();
    }

    @Test
    void givenUnreadableCreated_whenCounted_thenOutcomeUnreadableWithUnknownSource() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        new MetricsFacade(registry).unreadable(PutOutcome.CREATED);

        assertThat(registry.get("failed.events.archived")
                .tags("source_service", "unknown", "error_code", "UNREADABLE", "outcome", "unreadable")
                .counter().count()).isEqualTo(1.0);
    }

    @Test
    void givenUnreadableDuplicate_whenCounted_thenOutcomeDuplicate() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        new MetricsFacade(registry).unreadable(PutOutcome.DUPLICATE);

        assertThat(registry.get("failed.events.archived").tags("outcome", "duplicate").counter().count())
                .isEqualTo(1.0);
    }

    @Test
    void givenArchiveError_whenRecorded_thenCounterWithReason() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MetricsFacade metrics = new MetricsFacade(registry);

        metrics.archiveError("s3_unavailable");
        metrics.archiveError("s3_unavailable");

        assertThat(registry.get("failed.events.archive.errors").tags("reason", "s3_unavailable")
                .counter().count()).isEqualTo(2.0);
    }

    @Test
    void givenMetrics_whenRegistered_thenNoDeviceIdLabel() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        new MetricsFacade(registry).archived("device-collector-service", "PROCESSING_FAILED", PutOutcome.CREATED);

        assertThat(registry.find("failed.events.archived").counters())
                .allSatisfy(counter -> assertThat(counter.getId().getTag("device_id")).isNull());
    }
}
