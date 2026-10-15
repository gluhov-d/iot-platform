package com.github.gluhov.iotplatform.eventscollector.domain.service;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.eventscollector.domain.model.DeviceEventRecord;
import com.github.gluhov.iotplatform.eventscollector.domain.model.OutboxRecord;
import com.github.gluhov.iotplatform.eventscollector.domain.model.TraceRef;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

@Component
public class DeviceEventMapper {

    public DeviceEventRecord toEventRecord(DeviceEvent event, Instant ingestedAt) {
        return new DeviceEventRecord(
                event.getDeviceId(),
                event.getEventId(),
                eventDate(event.getTimestampMs()),
                event.getTimestampMs(),
                event.getEventType(),
                ingestedAt);
    }

    public OutboxRecord toInitialOutbox(DeviceEvent event, Instant now, TraceRef trace) {
        return OutboxRecord.initial(event.getEventId(), event.getDeviceId(), event.getTimestampMs(), now, trace);
    }

    /**
     * Дата события в UTC, а не в зоне JVM: иначе события около полуночи попадали бы
     * в разные партиции в зависимости от настроек сервера.
     */
    public static LocalDate eventDate(long timestampMs) {
        return LocalDate.ofInstant(Instant.ofEpochMilli(timestampMs), ZoneOffset.UTC);
    }
}
