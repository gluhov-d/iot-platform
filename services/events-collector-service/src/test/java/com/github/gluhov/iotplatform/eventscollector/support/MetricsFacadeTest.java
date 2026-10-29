package com.github.gluhov.iotplatform.eventscollector.support;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MetricsFacadeTest {

    @Test
    @DisplayName("Update every metric then Prometheus names match the task")
    void givenAllMetricsUpdated_whenScrape_thenRequiredPrometheusNames() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MetricsFacade metricsFacade = new MetricsFacade(registry);

        metricsFacade.eventProcessed();
        metricsFacade.eventPersistFailed();
        metricsFacade.outboxCandidateCreated();
        metricsFacade.publishSucceeded();
        metricsFacade.publishFailed();
        metricsFacade.publishSkipped();
        metricsFacade.pendingCount(42);

        // outbox_candidate_total переименовывается в outbox_candidate_created_total в prometheus.yaml
        assertThat(registry.scrape())
                .contains("events_processed_total 1.0")
                .contains("events_persist_fail_total 1.0")
                .contains("outbox_candidate_total 1.0")
                .contains("outbox_publish_success_total 1.0")
                .contains("outbox_publish_fail_total 1.0")
                .contains("outbox_publish_skipped_total 1.0")
                .contains("outbox_pending_count 42.0");
    }
}
