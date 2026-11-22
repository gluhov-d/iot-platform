package com.github.gluhov.iotplatform.failedeventsprocessor.adapter.in.kafka;

import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveDeniedException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveUnavailableException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.UnreadableRecord;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.service.ArchiveService;
import com.github.gluhov.iotplatform.failedeventsprocessor.support.MetricsFacade;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Восстановитель обработчика ошибок: запись failed-events, которую не удалось разобрать, уходит в архив как
 * {@code <yyyy>/<MM>/<dd>/<тема>-<раздел>-<смещение>.json} (ключ тот же, что у разобранной записи, ADR-0014) с исходными байтами (base64) и причиной (FR-010). Байты берутся из DeserializationException
 * (ErrorHandlingDeserializer) и из заголовков Spring с ошибкой разбора. Исключение записи пробрасывается:
 * смещение не подтверждается, запись придёт снова.
 */
@Component
public class UnreadableRecorder implements ConsumerRecordRecoverer {

    private static final LogAccessor LOG = new LogAccessor(UnreadableRecorder.class);
    private static final String SPRING_HEADER_PREFIX = "springDeserializerException";
    static final int REASON_MAX_LENGTH = 2000;

    private final ArchiveService archive;
    private final MetricsFacade metrics;

    public UnreadableRecorder(ArchiveService archive, MetricsFacade metrics) {
        this.archive = archive;
        this.metrics = metrics;
    }

    // Имена заголовков Spring помечены к удалению, но в spring-kafka 4.0.3 это единственный способ достать исключение
    @Override
    @SuppressWarnings("removal")
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        DeserializationException failure = find(exception, DeserializationException.class);
        DeserializationException valueFailure = header(record, SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER);
        DeserializationException keyFailure = header(record, SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER);
        if (valueFailure == null && failure != null && !failure.isKey()) {
            valueFailure = failure;
        }
        if (keyFailure == null && failure != null && failure.isKey()) {
            keyFailure = failure;
        }

        UnreadableRecord unreadable = new UnreadableRecord(
                record.topic(),
                record.partition(),
                record.offset(),
                Instant.ofEpochMilli(record.timestamp()),
                reason(failure != null ? failure : exception),
                keyBytes(record, keyFailure),
                valueFailure == null ? null : valueFailure.getData(),
                headers(record));
        try {
            metrics.unreadable(archive.archiveUnreadable(unreadable));
        } catch (ArchiveUnavailableException e) {
            metrics.archiveError("s3_unavailable");
            throw e;
        } catch (ArchiveDeniedException e) {
            metrics.archiveError("s3_denied");
            throw e;
        }
    }

    /** Ключ записи - строка: байты - UTF-8 того, что вернул StringDeserializer; при ошибке разбора ключа - исходные байты. */
    private static byte[] keyBytes(ConsumerRecord<?, ?> record, DeserializationException keyFailure) {
        if (keyFailure != null && keyFailure.getData() != null) {
            return keyFailure.getData();
        }
        return record.key() == null ? null : record.key().toString().getBytes(StandardCharsets.UTF_8);
    }

    private static DeserializationException header(ConsumerRecord<?, ?> record, String name) {
        return SerializationUtils.getExceptionFromHeader(record, name, LOG);
    }

    /** Все заголовки записи, кроме служебных заголовков Spring (ошибка разбора хранится в них в виде объекта Java). */
    private static Map<String, byte[]> headers(ConsumerRecord<?, ?> record) {
        Map<String, byte[]> headers = new HashMap<>();
        for (Header header : record.headers()) {
            if (!header.key().startsWith(SPRING_HEADER_PREFIX)) {
                headers.put(header.key(), header.value());
            }
        }
        return headers;
    }

    private static String reason(Throwable exception) {
        Throwable root = exception;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String text = root.getClass().getSimpleName() + ": " + root.getMessage();
        return text.length() > REASON_MAX_LENGTH ? text.substring(0, REASON_MAX_LENGTH) : text;
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
