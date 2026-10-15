package com.github.gluhov.iotplatform.eventscollector.service.domain.model;

import com.github.gluhov.iotplatform.eventscollector.domain.model.OutboxPolicy;
import com.github.gluhov.iotplatform.eventscollector.domain.model.OutboxRecord;
import com.github.gluhov.iotplatform.eventscollector.domain.model.TraceRef;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxRecordTest {
    private static final Instant T0 = Instant.parse("2026-03-01T00:00:00Z");

    @Test
    @DisplayName("Create initial record for redelivered event then version stays constant")
    void givenRedeliveredEvent_whenCreateInitial_thenVersionCannotOverrideLaterState() {
        OutboxRecord first = OutboxRecord.initial("e", "d", 1L, T0, TraceRef.NONE);
        OutboxRecord redelivered = OutboxRecord.initial("e", "d", 1L, T0.plusSeconds(60), TraceRef.NONE);

        assertThat(first.stateVersion()).isEqualTo(redelivered.stateVersion()).isEqualTo(1L);
        assertThat(first.sent(T0).stateVersion()).isGreaterThan(redelivered.stateVersion());
    }

    @Test
    @DisplayName("Mark failed then long error truncated and null error becomes empty")
    void givenLongOrNullError_whenFailed_thenErrorTruncatedOrEmpty() {
        OutboxRecord record = OutboxRecord.initial("e", "d", 1L, T0, TraceRef.NONE);

        assertThat(record.failed(T0, "x".repeat(5000)).lastError()).hasSize(OutboxRecord.MAX_ERROR_LENGTH);
        assertThat(record.failed(T0, null).lastError()).isEmpty();
    }

    @Test
    @DisplayName("Record created with ingestion trace then every next version keeps it")
    void givenTraceRecord_whenTransition_thenTraceKept() {
        TraceRef ingestion = new TraceRef("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7");
        OutboxRecord record = OutboxRecord.initial("e", "d", 1L, T0, ingestion);

        assertThat(record.failed(T0, "timeout").traceRef()).isEqualTo(ingestion);
        assertThat(record.failed(T0, "timeout").sent(T0).traceRef()).isEqualTo(ingestion);
    }

    @Test
    @DisplayName("Create policy with non-positive values then exception")
    void givenNonPositiveValues_whenCreatePolicy_thenThrowException() {
        assertThatThrownBy(() -> new OutboxPolicy(0, 10, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OutboxPolicy(1, 10, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
