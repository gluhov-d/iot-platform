package com.github.gluhov.iotplatform.devicecollector.support;

import com.github.gluhov.iotplatform.devicecollector.domain.model.FailureReason;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessingOutcome;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StorageFailureReason;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class MetricsFacadeTest {
    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    private final MetricsFacade metrics = new MetricsFacade(registry);

    @Test
    @DisplayName("Counters with known labels are exported from zero under the names required by the task")
    void givenNewFacade_whenScrape_thenRequiredCountersPresent() {
        String scrape = registry.scrape();

        assertThat(scrape)
                .contains("devices_messages_received_total 0.0")
                .contains("devices_messages_processed_total{outcome=\"applied\"} 0.0")
                .contains("devices_messages_processed_total{outcome=\"stale\"} 0.0")
                .contains("devices_messages_duplicate_total 0.0")
                .contains("devices_messages_failed_total{reason=\"storage\"} 0.0")
                .contains("devices_processing_errors_total{reason=\"storage\"} 0.0")
                .contains("devices_shard_route_fail_total{reason=\"connection\",shard=\"unknown\"} 0.0")
                .contains("devices_dlt_published_total{reason=\"invalid\"} 0.0");
    }

    @Test
    @DisplayName("Duplicate is counted separately from processed; timers and shard route failures carry the shard")
    void givenRecordedValues_whenScrape_thenExportedWithLabels() {
        metrics.received();
        metrics.received();
        metrics.processed(ProcessingOutcome.APPLIED);
        metrics.processed(ProcessingOutcome.DUPLICATE);
        metrics.failed(FailureReason.STORAGE);
        metrics.processingError(FailureReason.STORAGE);
        metrics.dltPublished(FailureReason.STORAGE);
        metrics.processingTime("applied", Duration.ofMillis(12));
        metrics.dbWrite("shard_1", true, Duration.ofMillis(5));
        metrics.dbWrite("unknown", false, Duration.ofMillis(3000));
        metrics.shardRouteFailed("shard_1", StorageFailureReason.CONNECTION);

        String scrape = registry.scrape();

        assertThat(scrape)
                .contains("devices_messages_received_total 2.0")
                .contains("devices_messages_processed_total{outcome=\"applied\"} 1.0")
                .contains("devices_messages_duplicate_total 1.0")
                .contains("devices_messages_failed_total{reason=\"storage\"} 1.0")
                .contains("devices_processing_errors_total{reason=\"storage\"} 1.0")
                .contains("devices_dlt_published_total{reason=\"storage\"} 1.0")
                .contains("devices_processing_seconds_count{outcome=\"applied\"} 1")
                .contains("devices_db_write_seconds_count{result=\"success\",shard=\"shard_1\"} 1")
                .contains("devices_db_write_seconds_count{result=\"error\",shard=\"unknown\"} 1")
                .contains("devices_shard_route_fail_total{reason=\"connection\",shard=\"shard_1\"} 1.0");
    }
}