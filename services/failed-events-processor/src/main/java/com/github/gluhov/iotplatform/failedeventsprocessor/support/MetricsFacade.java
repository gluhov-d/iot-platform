package com.github.gluhov.iotplatform.failedeventsprocessor.support;

import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.PutOutcome;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.Locale;

/**
 * Метрики процессора. Метки без deviceId (кардинальность). Таймер s3.put-object - Observation в адаптере S3.
 */
@Component
public class MetricsFacade {

    private static final String UNKNOWN_SOURCE = "unknown";
    private static final String UNREADABLE_CODE = "UNREADABLE";

    private final MeterRegistry registry;

    public MetricsFacade(MeterRegistry registry) {
        this.registry = registry;
    }

    public void archived(String sourceService, String errorCode, PutOutcome outcome) {
        Counter.builder("failed.events.archived")
                .description("Failed events archived: created, duplicate (repeated delivery), unreadable")
                .tag("source_service", sourceService)
                .tag("error_code", errorCode)
                .tag("outcome", outcome.name().toLowerCase(Locale.ROOT))
                .register(registry)
                .increment();
    }

    /**
     * Нечитаемая запись: источник и код неизвестны, поэтому метки - {@code unknown} и {@code UNREADABLE}.
     * Повторная доставка того же объекта (412) считается duplicate, как у разобранных записей.
     */
    public void unreadable(PutOutcome outcome) {
        String label = outcome == PutOutcome.DUPLICATE ? "duplicate" : "unreadable";
        Counter.builder("failed.events.archived")
                .description("Failed events archived: created, duplicate (repeated delivery), unreadable")
                .tag("source_service", UNKNOWN_SOURCE)
                .tag("error_code", UNREADABLE_CODE)
                .tag("outcome", label)
                .register(registry)
                .increment();
    }

    public void archiveError(String reason) {
        Counter.builder("failed.events.archive.errors")
                .description("Archive write attempts that failed: s3_unavailable, s3_denied, s3_error")
                .tag("reason", reason)
                .register(registry)
                .increment();
    }
}
