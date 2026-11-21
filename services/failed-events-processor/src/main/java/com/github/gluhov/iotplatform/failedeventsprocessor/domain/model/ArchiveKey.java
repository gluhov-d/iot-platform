package com.github.gluhov.iotplatform.failedeventsprocessor.domain.model;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Ключ объекта ошибки: {@code <sourceService>/<yyyy>/<MM>/<dd>/<errorCode>/<sourceTopic>-<sourcePartition>-<sourceOffset>.json}.
 * Дата - время исходного сообщения в UTC: повторные публикации одного сообщения попадают в один ключ (FR-008).
 * Символы темы вне {@code [A-Za-z0-9._-]} заменяются на {@code _}, чтобы ключ был безопасен для S3 и путей.
 */
public final class ArchiveKey {

    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy/MM/dd").withZone(ZoneOffset.UTC);

    private ArchiveKey() {
    }

    public static String of(FailedRecord record) {
        return record.sourceService() + "/"
                + DATE.format(record.sourceTimestamp()) + "/"
                + record.errorCode() + "/"
                + safe(record.sourceTopic()) + "-" + record.sourcePartition() + "-" + record.sourceOffset()
                + ".json";
    }

    /** Нечитаемая запись: дата - время записи в failed-events (UTC), координаты - её место в failed-events (FR-010). */
    public static String unreadable(UnreadableRecord record) {
        return "unreadable/" + DATE.format(record.recordTimestamp()) + "/"
                + "failed-events-" + record.partition() + "-" + record.offset() + ".json";
    }

    private static String safe(String topic) {
        return topic.replaceAll("[^A-Za-z0-9._-]", "_");
    }
}
