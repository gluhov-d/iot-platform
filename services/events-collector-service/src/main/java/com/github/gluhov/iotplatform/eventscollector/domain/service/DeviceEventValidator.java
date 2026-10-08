package com.github.gluhov.iotplatform.eventscollector.domain.service;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.InvalidEventException;
import lombok.RequiredArgsConstructor;

import java.time.Clock;
import java.time.Duration;

@RequiredArgsConstructor
public class DeviceEventValidator {

    public static final Duration MAX_FUTURE_SKEW = Duration.ofDays(1);
    private final Clock clock;
    
    public void validate(DeviceEvent event) {
        if (event == null) {
            throw new InvalidEventException("пустое сообщение");
        }
        requireText(event.getEventId(), "eventId");
        requireText(event.getDeviceId(), "deviceId");
        requireText(event.getEventType(), "eventType");
        
        long timestampMs = event.getTimestampMs();
        if (timestampMs <= 0) {
            throw new InvalidEventException("timestampMs должен быть положительным: " + timestampMs);
        }
        long latestAllowed = clock.millis() + MAX_FUTURE_SKEW.toMillis();
        if (timestampMs > latestAllowed) {
            throw new InvalidEventException("timestampMs в будущем больше чем на " + MAX_FUTURE_SKEW + ": " + timestampMs);
        }
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new InvalidEventException(field + " не заполнен");
        }
    }
}
