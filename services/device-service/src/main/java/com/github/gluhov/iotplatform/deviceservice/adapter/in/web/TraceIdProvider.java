package com.github.gluhov.iotplatform.deviceservice.adapter.in.web;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Идентификатор трассировки для тела ошибки. Берётся из текущего span; без трассировки (например, в модульных тестах)
 * выдаётся случайный идентификатор той же формы (32 hex-символа), чтобы поле traceId всегда было заполнено.
 */
@Component
public class TraceIdProvider {

    private final ObjectProvider<Tracer> tracer;

    public TraceIdProvider(ObjectProvider<Tracer> tracer) {
        this.tracer = tracer;
    }

    public String current() {
        Tracer current = tracer.getIfAvailable();
        if (current != null) {
            Span span = current.currentSpan();
            if (span != null) {
                return span.context().traceId();
            }
        }
        return UUID.randomUUID().toString().replace("-", "");
    }
}
