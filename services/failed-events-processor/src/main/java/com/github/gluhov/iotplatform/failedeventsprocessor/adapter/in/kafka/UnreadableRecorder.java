package com.github.gluhov.iotplatform.failedeventsprocessor.adapter.in.kafka;

import com.github.gluhov.iotplatform.failedeventsprocessor.config.RawKeepingDeserializer;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveDeniedException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveUnavailableException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.HeaderEntry;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.UnreadableRecord;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.service.ArchiveService;
import com.github.gluhov.iotplatform.failedeventsprocessor.support.MetricsFacade;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Восстановитель обработчика ошибок: запись failed-events, которую не удалось разобрать, уходит в архив как
 * {@code <yyyy>/<MM>/<dd>/<тема>-<раздел>-<смещение>.json} (ключ тот же, что у разобранной записи, ADR-0014) с исходными байтами (base64) и причиной (FR-010).
 * Исходные байты ключа и значения берутся только из заголовков {@code x-raw-key} и {@code x-raw-value}, которые
 * обёртка {@link RawKeepingDeserializer} кладёт в запись до разбора: это работает и для ошибок, которые Spring Kafka
 * не оборачивает в DeserializationException (например, ClassCastException). Исключение записи пробрасывается:
 * смещение не подтверждается, запись придёт снова.
 */
@Component
public class UnreadableRecorder implements ConsumerRecordRecoverer {

    private static final String SPRING_HEADER_PREFIX = "springDeserializerException";
    static final int REASON_MAX_LENGTH = 2000;

    private final ArchiveService archive;
    private final MetricsFacade metrics;

    public UnreadableRecorder(ArchiveService archive, MetricsFacade metrics) {
        this.archive = archive;
        this.metrics = metrics;
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        DeserializationException failure = find(exception, DeserializationException.class);

        UnreadableRecord unreadable = new UnreadableRecord(
                record.topic(),
                record.partition(),
                record.offset(),
                Instant.ofEpochMilli(record.timestamp()),
                reason(failure != null ? failure : exception),
                rawBytes(record, RawKeepingDeserializer.RAW_KEY_HEADER),
                rawBytes(record, RawKeepingDeserializer.RAW_VALUE_HEADER),
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

    /** Исходные байты, сохранённые обёрткой до разбора; null - байтов в записи не было (пустой ключ или значение). */
    private static byte[] rawBytes(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : header.value();
    }

    /**
     * Все заголовки записи по порядку, с повторами и значениями null, без служебных: заголовки Spring (ошибка разбора
     * хранится в них в виде объекта Java) и сырые байты x-raw-key, x-raw-value, которые уже вынесены в поля объекта.
     */
    private static List<HeaderEntry> headers(ConsumerRecord<?, ?> record) {
        List<HeaderEntry> headers = new ArrayList<>();
        for (Header header : record.headers()) {
            if (!isService(header.key())) {
                headers.add(new HeaderEntry(header.key(), header.value()));
            }
        }
        return headers;
    }

    private static boolean isService(String name) {
        return name.startsWith(SPRING_HEADER_PREFIX)
                || name.equals(RawKeepingDeserializer.RAW_KEY_HEADER)
                || name.equals(RawKeepingDeserializer.RAW_VALUE_HEADER);
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
