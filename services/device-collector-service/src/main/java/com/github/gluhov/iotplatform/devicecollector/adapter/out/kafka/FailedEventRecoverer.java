package com.github.gluhov.iotplatform.devicecollector.adapter.out.kafka;

import com.github.gluhov.iotplatform.devicecollector.config.KafkaProperties;
import com.github.gluhov.iotplatform.devicecollector.config.RawKeepingDeserializer;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.DeviceNotStoredException;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.InvalidDeviceInfoException;
import com.github.gluhov.iotplatform.devicecollector.domain.port.out.DeviceMetrics;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.ErrorCode;
import com.github.gluhov.iotplatform.events.avro.ErrorType;
import com.github.gluhov.iotplatform.events.avro.FailedEvent;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.MDC;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Сообщение, которое приём не смог обработать, - в failed-events (ADR-0013). Публикация синхронная, с тайм-аутом
 * FAILED_EVENTS_PUBLISH_TIMEOUT: сбой публикации - исключение, смещение не подтверждается, сообщение обработается снова.
 * Исходные байты берутся из заголовков x-raw-key и x-raw-value (RawKeepingDeserializer), а не пересериализуются.
 */
@Component
@Slf4j
public class FailedEventRecoverer implements ConsumerRecordRecoverer {
    static final String SOURCE_SERVICE = "device-collector-service";
    static final int DESCRIPTION_MAX_LENGTH = 2000;
    private static final String TRACEPARENT = "traceparent";

    private final KafkaTemplate<String, Object> template;
    private final Tracer tracer;
    private final DeviceMetrics metrics;
    private final Clock clock;
    private final String failedEventsTopic;
    private final long publishTimeoutMs;

    @SuppressWarnings("unchecked")
    public FailedEventRecoverer(KafkaTemplate<?, ?> template, Tracer tracer, DeviceMetrics metrics, Clock clock,
                                KafkaProperties kafka) {
        this.template = (KafkaTemplate<String, Object>) template;
        this.tracer = tracer;
        this.metrics = metrics;
        this.clock = clock;
        this.failedEventsTopic = kafka.failedEventsTopic();
        this.publishTimeoutMs = kafka.failedEventsPublishTimeout().toMillis();
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        Headers headers = record.headers();
        DeserializationException deserialization = find(exception, DeserializationException.class);
        Classification classification = classify(exception, deserialization, record);

        byte[] key = rawBytes(headers, RawKeepingDeserializer.RAW_KEY_HEADER);
        byte[] value = rawBytes(headers, RawKeepingDeserializer.RAW_VALUE_HEADER);
        if (deserialization != null) {
            // Ошибка разбора без заголовка (обёртка не сработала) - байты из исключения Spring
            if (deserialization.isKey() && key == null) {
                key = deserialization.getData();
            }
            if (!deserialization.isKey() && value == null) {
                value = deserialization.getData();
            }
        }
        String rootMessage = rootMessage(exception);
        String sourceKey = record.topic() + "-" + record.partition() + "-" + record.offset();

        FailedEvent event = FailedEvent.newBuilder()
                .setSourceService(SOURCE_SERVICE)
                .setSourceTopic(record.topic())
                .setSourcePartition(record.partition())
                .setSourceOffset(record.offset())
                .setSourceTimestamp(Instant.ofEpochMilli(record.timestamp()))
                .setFailedAt(clock.instant())
                .setErrorCode(classification.code())
                .setErrorType(classification.type())
                .setDescription(truncate(rootMessage))
                .setTraceId(traceId(headers))
                .setKey(key == null ? null : ByteBuffer.wrap(key))
                .setValue(value == null ? null : ByteBuffer.wrap(value))
                .setHeaders(copyHeaders(headers))
                .setAttempts(classification.attempts(deliveryAttempt(headers)))
                .setDeviceId(deviceIdOf(record))
                .build();

        try {
            template.send(new ProducerRecord<>(failedEventsTopic, sourceKey, event))
                    .get(publishTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Не опубликовано в " + failedEventsTopic, e);
        } catch (Exception e) {
            throw new IllegalStateException("Не опубликовано в " + failedEventsTopic, e);
        }
        metrics.failedEventPublished(event.getErrorCode(), event.getErrorType());
        logPublished(record, exception, classification.code(), rootMessage);
    }

    /** Код и тип по исключению (таблица «исключение → код» data-model.md) и число попыток. */
    private static Classification classify(Exception exception, DeserializationException deserialization,
                                           ConsumerRecord<?, ?> record) {
        if (deserialization != null) {
            return new Classification(ErrorCode.DESERIALIZATION_FAILED, ErrorType.PERMANENT, false);
        }
        if (find(exception, InvalidDeviceInfoException.class) != null) {
            return new Classification(ErrorCode.VALIDATION_FAILED, ErrorType.PERMANENT, false);
        }
        if (find(exception, DeviceNotStoredException.class) != null) {
            return new Classification(ErrorCode.STORAGE_UNAVAILABLE, ErrorType.TRANSIENT, true);
        }
        return new Classification(ErrorCode.PROCESSING_FAILED, ErrorType.TRANSIENT, true);
    }

    private static String deviceIdOf(ConsumerRecord<?, ?> record) {
        return record.value() instanceof DeviceInfo info ? info.getDeviceId() : null;
    }

    /** Текущий спан, затем traceparent сообщения, иначе null (у сообщения без трассы). */
    private String traceId(Headers headers) {
        Span span = tracer.currentSpan();
        if (span != null) {
            return span.context().traceId();
        }
        return traceIdOf(rawString(headers, TRACEPARENT));
    }

    /** traceparent = 00-&lt;traceId 32 hex&gt;-&lt;spanId&gt;-&lt;flags&gt; */
    static String traceIdOf(String traceparent) {
        if (traceparent == null) {
            return null;
        }
        String[] parts = traceparent.split("-");
        return parts.length >= 4 ? parts[1] : null;
    }

    /** Заголовки исходного сообщения без служебных заголовков Spring Kafka и обёртки. */
    private static Map<String, ByteBuffer> copyHeaders(Headers headers) {
        Map<String, ByteBuffer> copied = new HashMap<>();
        for (Header header : headers) {
            if (isServiceHeader(header.key()) || header.value() == null) {
                continue;
            }
            copied.put(header.key(), ByteBuffer.wrap(header.value()));
        }
        return copied;
    }

    private static boolean isServiceHeader(String name) {
        return name.startsWith("spring") || name.startsWith("kafka_")
                || RawKeepingDeserializer.RAW_KEY_HEADER.equals(name)
                || RawKeepingDeserializer.RAW_VALUE_HEADER.equals(name);
    }

    /** Номер доставки из kafka_deliveryAttempt; нет заголовка - первая доставка. */
    static int deliveryAttempt(Headers headers) {
        byte[] value = rawBytes(headers, KafkaHeaders.DELIVERY_ATTEMPT);
        return value == null || value.length != Integer.BYTES ? 1 : ByteBuffer.wrap(value).getInt();
    }

    private void logPublished(ConsumerRecord<?, ?> record, Exception exception, ErrorCode code, String rootMessage) {
        try (var topic = MDC.putCloseable("topic", record.topic());
             var partition = MDC.putCloseable("partition", String.valueOf(record.partition()));
             var offset = MDC.putCloseable("offset", String.valueOf(record.offset()));
             var deviceId = MDC.putCloseable("deviceId", record.key() == null ? null : String.valueOf(record.key()));
             var shard = MDC.putCloseable("shard", shardOf(exception));
             var status = MDC.putCloseable("status", "FAILED_EVENT");
             var errorClass = MDC.putCloseable("errorClass", rootClassName(exception));
             var errorMessage = MDC.putCloseable("errorMessage", rootMessageText(exception))) {
            log.error("Message moved to {} ({}): {}", failedEventsTopic, code.name(), rootMessage);
        }
    }

    private static String shardOf(Throwable exception) {
        DeviceNotStoredException notStored = find(exception, DeviceNotStoredException.class);
        return notStored == null ? "unknown" : notStored.shard();
    }

    private static String rootClassName(Throwable exception) {
        return rootCause(exception).getClass().getName();
    }

    private static String rootMessageText(Throwable exception) {
        return String.valueOf(rootCause(exception).getMessage());
    }

    /** Класс и сообщение корневой причины, без стека. */
    static String rootMessage(Throwable exception) {
        Throwable root = rootCause(exception);
        return root.getClass().getName() + ": " + root.getMessage();
    }

    static String truncate(String text) {
        return text.length() > DESCRIPTION_MAX_LENGTH ? text.substring(0, DESCRIPTION_MAX_LENGTH) : text;
    }

    private static Throwable rootCause(Throwable exception) {
        Throwable root = exception;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root;
    }

    private static <T extends Throwable> T find(Throwable exception, Class<T> type) {
        for (Throwable e = exception; e != null; e = e.getCause() == e ? null : e.getCause()) {
            if (type.isInstance(e)) {
                return type.cast(e);
            }
        }
        return null;
    }

    private static byte[] rawBytes(Headers headers, String name) {
        Header header = headers.lastHeader(name);
        return header == null ? null : header.value();
    }

    private static String rawString(Headers headers, String name) {
        byte[] value = rawBytes(headers, name);
        return value == null ? null : new String(value, StandardCharsets.US_ASCII);
    }

    /** Классификация ошибки: код, тип и число попыток (для повторяемых ошибок - номер доставки). */
    private record Classification(ErrorCode code, ErrorType type, boolean retried) {
        int attempts(int deliveries) {
            return retried ? deliveries : 1;
        }
    }
}
