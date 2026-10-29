package com.github.gluhov.iotplatform.eventscollector.domain.model;

// Ссылка на спан, который продолжит публикация: trace_id и span_id из device_outbox
public record TraceRef(String traceId, String spanId) {
    public static final TraceRef NONE = new TraceRef("", "");

    public boolean isEmpty() {
        return traceId.isEmpty() || spanId.isEmpty();
    }
}