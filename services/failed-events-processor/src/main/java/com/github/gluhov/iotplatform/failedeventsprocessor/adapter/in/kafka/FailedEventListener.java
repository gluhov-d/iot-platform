package com.github.gluhov.iotplatform.failedeventsprocessor.adapter.in.kafka;

import com.github.gluhov.iotplatform.events.avro.FailedEvent;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveDeniedException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveUnavailableException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.ArchiveCoordinates;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.FailedRecord;
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
import java.util.HashMap;
import java.util.Map;

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
    public void onMessage(ConsumerRecord<String, FailedEvent> record, Acknowledgment ack) {
        FailedEvent event = record.value();
        if (event == null) {
            // Контейнер (checkDeserExWhenValueNull) передаёт сюда null только вместе с причиной разбора.
            // Исключение уходит в обработчик ошибок: он решает про повтор или unreadable (UnreadableRecorder).
            throw new DeserializationException("Значение записи failed-events не разобрано", null, false, null);
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
        Map<String, byte[]> headers = new HashMap<>();
        event.getHeaders().forEach((name, bytes) -> headers.put(name, bytes(bytes)));
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
                headers,
                event.getAttempts(),
                event.getDeviceId());
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
