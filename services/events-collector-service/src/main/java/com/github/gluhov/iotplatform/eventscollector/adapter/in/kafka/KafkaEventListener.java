package com.github.gluhov.iotplatform.eventscollector.adapter.in.kafka;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.InvalidEventException;
import com.github.gluhov.iotplatform.eventscollector.domain.port.in.IngestEventUseCase;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Objects;

@Component
@Slf4j
@RequiredArgsConstructor
public class KafkaEventListener {
    private final IngestEventUseCase ingestEvent;

    @KafkaListener(
            id = "events-listener",
            topics = "${app.kafka.events-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            batch = "false"
    )
    public void onEvent(ConsumerRecord<String, DeviceEvent> record, Acknowledgment ack) {
        DeviceEvent event = record.value();
        try (var topic = MDC.putCloseable("topic", record.topic());
            var partition = MDC.putCloseable("partition", String.valueOf(record.partition()));
            var offset = MDC.putCloseable("offset", String.valueOf(record.offset()));
            var eventId = MDC.putCloseable("eventId", event == null? null: event.getEventId());
            var deviceId = MDC.putCloseable("deviceId", event == null? null: event.getDeviceId())) {
            if (event != null && record.key() != null && !Objects.equals(record.key(), event.getDeviceId())) {
                log.warn("Message key {} does not match event deviceId", record.key());
            }
            try {
                ingestEvent.ingest(event);
            } catch (RuntimeException e) {
                logFailure(e);
                throw e;
            }
            // Offset подтверждается только после записи в device_events и device_outbox
            ack.acknowledge();
            try (var status = MDC.putCloseable("status", "PROCESSED")){
                log.info("Event stored, offset acknowledged");
            }
        }
    }

    private static void logFailure(RuntimeException e) {
        String status = e instanceof InvalidEventException ? "REJECTED" : "FAILED";
        try (var s = MDC.putCloseable("status", status);
            var c = MDC.putCloseable("errorClass", e.getClass().getName());
            var m = MDC.putCloseable("errorMessage", String.valueOf(e.getMessage()))) {
            log.warn("Event not processed, offset not acknowledged");
        }
    }
}
