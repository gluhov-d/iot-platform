package com.github.gluhov.iotplatform.failedeventsprocessor.domain.model;

import java.time.Instant;
import java.util.List;

/**
 * Запись failed-events в виде, с которым работает домен: поля FailedEvent без типов Avro.
 * Байты ключа, значения и заголовков - исходные, без изменений.
 */
public record FailedRecord(
        String sourceService,
        String sourceTopic,
        int sourcePartition,
        long sourceOffset,
        Instant sourceTimestamp,
        Instant failedAt,
        String errorCode,
        String errorType,
        String description,
        String traceId,
        byte[] key,
        byte[] value,
        List<HeaderEntry> headers,
        int attempts,
        String deviceId) {
}
