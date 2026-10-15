package com.github.gluhov.iotplatform.eventscollector.domain.port.out;

import com.github.gluhov.iotplatform.eventscollector.domain.model.TraceRef;

import java.util.function.Supplier;

public interface TraceContextCarrier {

    // Текущий спан (traceId и spanId) или TraceRef.NONE, если трассировки нет
    TraceRef current();

    // Выполнить work так, чтобы новые спаны стали дочерними к parent; при пустом parent - как обычно
    <T> T continueTrace(TraceRef parent, Supplier<T> work);
}