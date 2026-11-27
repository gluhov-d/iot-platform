package com.github.gluhov.iotplatform.eventscollector.adapter.out.kafka;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.events.avro.ErrorCode;
import com.github.gluhov.iotplatform.events.avro.ErrorType;
import com.github.gluhov.iotplatform.events.avro.FailedEvent;
import com.github.gluhov.iotplatform.events.avro.SourceHeader;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.EventPersistenceException;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.InvalidEventException;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.CollectorMetrics;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.github.gluhov.iotplatform.eventscollector.config.RawKeepingDeserializer.RAW_KEY_HEADER;
import static com.github.gluhov.iotplatform.eventscollector.config.RawKeepingDeserializer.RAW_VALUE_HEADER;

/**
 * Запись сообщения, которое сборщик не смог обработать, в тему failed-events. Вызывается обработчиком ошибок
 * контейнера после последней попытки (или сразу для постоянной ошибки).
 *
 * <p>Публикация синхронная: сбой - исключение, смещение исходного сообщения не подтверждается, и сообщение
 * обработается снова. Байты ключа и значения берутся из заголовков, которые кладёт RawKeepingDeserializer до разбора,
 * а не пересериализуются. Ключ сообщения failed-events - {@code <topic>-<partition>-<offset>} исходного.
 */
public class FailedEventRecoverer implements ConsumerRecordRecoverer {
    private static final int MAX_DESCRIPTION_LENGTH = 2000;
    private static final String TRACEPARENT = "traceparent";
    private static final int TRACE_ID_LENGTH = 32;

    private final KafkaOperations<String, Object> template;
    private final Tracer tracer;
    private final CollectorMetrics metrics;
    private final String failedEventsTopic;
    private final String sourceService;
    private final Duration publishTimeout;

    public FailedEventRecoverer(KafkaOperations<String, Object> template, Tracer tracer, CollectorMetrics metrics,
                                String failedEventsTopic, String sourceService, Duration publishTimeout) {
        this.template = template;
        this.tracer = tracer;
        this.metrics = metrics;
        this.failedEventsTopic = failedEventsTopic;
        this.sourceService = sourceService;
        this.publishTimeout = publishTimeout;
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        DeserializationException deserialization = find(exception, DeserializationException.class);
        ErrorCode code;
        ErrorType type;
        int attempts;
        if (deserialization != null) {
            code = ErrorCode.DESERIALIZATION_FAILED;
            type = ErrorType.PERMANENT;
            attempts = 1;
        } else if (find(exception, InvalidEventException.class) != null) {
            code = ErrorCode.VALIDATION_FAILED;
            type = ErrorType.PERMANENT;
            attempts = 1;
        } else if (find(exception, EventPersistenceException.class) != null) {
            code = ErrorCode.STORAGE_UNAVAILABLE;
            type = ErrorType.TRANSIENT;
            attempts = deliveryAttempt(record.headers());
        } else {
            code = ErrorCode.PROCESSING_FAILED;
            type = ErrorType.TRANSIENT;
            attempts = deliveryAttempt(record.headers());
        }

        Headers headers = record.headers();
        byte[] key = rawBytes(headers, RAW_KEY_HEADER, deserialization != null && deserialization.isKey()
                ? deserialization.getData() : null);
        byte[] value = rawBytes(headers, RAW_VALUE_HEADER, deserialization != null && !deserialization.isKey()
                ? deserialization.getData() : null);

        FailedEvent event = FailedEvent.newBuilder()
                .setSourceService(sourceService)
                .setSourceTopic(record.topic())
                .setSourcePartition(record.partition())
                .setSourceOffset(record.offset())
                .setSourceTimestamp(Instant.ofEpochMilli(record.timestamp()))
                .setFailedAt(Instant.now())
                .setErrorCode(code)
                .setErrorType(type)
                .setDescription(describe(exception))
                .setTraceId(traceId(headers))
                .setKey(key == null ? null : ByteBuffer.wrap(key))
                .setValue(value == null ? null : ByteBuffer.wrap(value))
                .setHeaders(sourceHeaders(headers))
                .setHeaderList(sourceHeaderList(headers))
                .setAttempts(attempts)
                .setDeviceId(record.value() instanceof DeviceEvent parsed ? parsed.getDeviceId() : null)
                .build();

        String sourceKey = record.topic() + "-" + record.partition() + "-" + record.offset();
        try {
            template.send(new ProducerRecord<>(failedEventsTopic, sourceKey, event))
                    .get(publishTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("не опубликовано в " + failedEventsTopic, e);
        } catch (Exception e) {
            throw new IllegalStateException("не опубликовано в " + failedEventsTopic, e);
        }
        metrics.failedEventPublished(code.name(), type.name());
    }

    // Номер доставки (заголовок kafka_deliveryAttempt, целое в 4 байтах); нет заголовка - первая доставка
    private static int deliveryAttempt(Headers headers) {
        Header header = headers.lastHeader(KafkaHeaders.DELIVERY_ATTEMPT);
        if (header == null || header.value() == null || header.value().length != Integer.BYTES) {
            return 1;
        }
        return ByteBuffer.wrap(header.value()).getInt();
    }

    private static byte[] rawBytes(Headers headers, String name, byte[] fallback) {
        Header header = headers.lastHeader(name);
        return header != null ? header.value() : fallback;
    }

    // Текущий спан - трасса входящего сообщения внутри обработчика ошибок; запасной путь - заголовок traceparent
    private String traceId(Headers headers) {
        Span current = tracer.currentSpan();
        if (current != null && current.context().traceId() != null) {
            return current.context().traceId();
        }
        return traceIdFromTraceparent(headerString(headers, TRACEPARENT));
    }

    /** traceparent = 00-&lt;traceId 32 hex&gt;-&lt;spanId&gt;-&lt;flags&gt; */
    static String traceIdFromTraceparent(String traceparent) {
        if (traceparent == null) {
            return null;
        }
        String[] parts = traceparent.split("-");
        return parts.length >= 4 && parts[1].length() == TRACE_ID_LENGTH ? parts[1] : null;
    }

    // Заголовки исходного сообщения без служебных: сырые байты (уже в key/value) и заголовки Spring Kafka.
    // Для читателей 1.4.0: по одному значению на имя (последнее), без значений null.
    private static Map<String, ByteBuffer> sourceHeaders(Headers headers) {
        Map<String, ByteBuffer> copied = new LinkedHashMap<>();
        for (Header header : headers) {
            if (header.value() == null || isServiceHeader(header.key())) {
                continue;
            }
            copied.put(header.key(), ByteBuffer.wrap(header.value()));
        }
        return copied;
    }

    // Все заголовки исходного сообщения по порядку, с повторами и значениями null (с 1.5.0); служебные - как в sourceHeaders
    private static List<SourceHeader> sourceHeaderList(Headers headers) {
        List<SourceHeader> list = new ArrayList<>();
        for (Header header : headers) {
            if (isServiceHeader(header.key())) {
                continue;
            }
            list.add(SourceHeader.newBuilder()
                    .setName(header.key())
                    .setValue(header.value() == null ? null : ByteBuffer.wrap(header.value()))
                    .build());
        }
        return list;
    }

    private static boolean isServiceHeader(String name) {
        return RAW_KEY_HEADER.equals(name) || RAW_VALUE_HEADER.equals(name)
                || name.startsWith("spring") || name.startsWith("kafka_");
    }

    private static String headerString(Headers headers, String name) {
        Header header = headers.lastHeader(name);
        return header == null || header.value() == null
                ? null : new String(header.value(), StandardCharsets.US_ASCII);
    }

    // «Класс корневой причины: сообщение», без стека, не длиннее 2000 символов
    static String describe(Throwable exception) {
        Throwable root = exception;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String description = root.getClass().getName() + ": " + root.getMessage();
        return description.length() > MAX_DESCRIPTION_LENGTH
                ? description.substring(0, MAX_DESCRIPTION_LENGTH) : description;
    }

    private static <T extends Throwable> T find(Throwable exception, Class<T> type) {
        for (Throwable e = exception; e != null; e = e.getCause() == e ? null : e.getCause()) {
            if (type.isInstance(e)) {
                return type.cast(e);
            }
        }
        return null;
    }
}
