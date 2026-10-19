package com.github.gluhov.iotplatform.eventscollector.support;

import com.github.gluhov.iotplatform.eventscollector.domain.model.TraceRef;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.TraceContextCarrier;
import io.micrometer.tracing.CurrentTraceContext;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;

@Component
@RequiredArgsConstructor
public class MicrometerTraceContextCarrier implements TraceContextCarrier {
    private final Tracer tracer;

    @Override
    public TraceRef current() {
        TraceContext context = tracer.currentTraceContext().context();
        return context == null ? TraceRef.NONE : new TraceRef(context.traceId(), context.spanId());
    }

    @Override
    public <T> T continueTrace(TraceRef parent, Supplier<T> work) {
        if (parent.isEmpty()) {
            return work.get();
        }
        // Сохраненный спан приема становится текущим: спаны, начатые внутри work, будут его детьми
        TraceContext context = tracer.traceContextBuilder()
                .traceId(parent.traceId())
                .spanId(parent.spanId())
                .sampled(true)
                .build();
        try (CurrentTraceContext.Scope ignored = tracer.currentTraceContext().newScope(context)) {
            return work.get();
        }
    }
}