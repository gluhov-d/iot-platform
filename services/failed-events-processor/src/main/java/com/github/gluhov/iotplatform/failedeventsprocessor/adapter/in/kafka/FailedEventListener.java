package com.github.gluhov.iotplatform.failedeventsprocessor.adapter.in.kafka;

import com.github.gluhov.iotplatform.events.avro.FailedEvent;
import com.github.gluhov.iotplatform.events.avro.SourceHeader;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveDeniedException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveUnavailableException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.ArchiveCoordinates;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.FailedRecord;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.HeaderEntry;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.PutOutcome;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.service.ArchiveService;
import com.github.gluhov.iotplatform.failedeventsprocessor.support.MetricsFacade;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Приём failed-events. Смещение подтверждается только после успешной записи объекта (или 412 - дубликат).
 * Исключения записи пробрасываются: обработчик ошибок повторит сообщение без подтверждения.
 */
@Component
public class FailedEventListener {

    private final ArchiveService archive;
    private final MetricsFacade metrics;
    private final Clock clock;

    public FailedEventListener(ArchiveService archive, MetricsFacade metrics, Clock clock) {
        this.archive = archive;
        this.metrics = metrics;
        this.clock = clock;
    }

    @KafkaListener(id = "failed-events-processor", topics = "${KAFKA_FAILED_EVENTS_TOPIC}",
            groupId = "${FAILED_EVENTS_CONSUMER_GROUP}")
    public void onMessage(ConsumerRecord<String, Object> record, Acknowledgment ack) {
        // Тип значения проверяет сам слушатель: корректный Avro другого типа и ошибка разбора (null) - нечитаемая запись.
        // Исключение уходит в обработчик ошибок: он решает про повтор или unreadable (UnreadableRecorder).
        if (!(record.value() instanceof FailedEvent event)) {
            throw new DeserializationException("Значение записи failed-events не FailedEvent", null, false, null);
        }
        FailedRecord failed = toRecord(event);
        PutOutcome outcome;
        try {
            outcome = archive.archive(failed, new ArchiveCoordinates(
                    record.topic(), record.partition(), record.offset(), Instant.ofEpochMilli(record.timestamp()),
                    clock.instant()));
        } catch (ArchiveUnavailableException e) {
            metrics.archiveError("s3_unavailable");
            throw e;
        } catch (ArchiveDeniedException e) {
            metrics.archiveError("s3_denied");
            throw e;
        }
        metrics.archived(failed.sourceService(), failed.errorCode(), outcome);
        ack.acknowledge();
    }

    private static FailedRecord toRecord(FailedEvent event) {
        return new FailedRecord(
                event.getSourceService(),
                event.getSourceTopic(),
                event.getSourcePartition(),
                event.getSourceOffset(),
                event.getSourceTimestamp(),
                event.getFailedAt(),
                event.getErrorCode().name(),
                event.getErrorType().name(),
                event.getDescription(),
                event.getTraceId(),
                bytes(event.getKey()),
                bytes(event.getValue()),
                headersOf(event),
                event.getAttempts(),
                event.getDeviceId());
    }

    /**
     * Заголовки записи: headerList (1.5.0) - все заголовки по порядку, с повторами и null; у записи 1.4.0 headerList = null,
     * тогда - map headers (по одному значению на имя, без null, R26).
     */
    private static List<HeaderEntry> headersOf(FailedEvent event) {
        List<HeaderEntry> headers = new ArrayList<>();
        if (event.getHeaderList() != null) {
            for (SourceHeader header : event.getHeaderList()) {
                headers.add(new HeaderEntry(header.getName(), bytes(header.getValue())));
            }
            return headers;
        }
        event.getHeaders().forEach((name, value) -> headers.add(new HeaderEntry(name, bytes(value))));
        return headers;
    }

    private static byte[] bytes(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }
}
