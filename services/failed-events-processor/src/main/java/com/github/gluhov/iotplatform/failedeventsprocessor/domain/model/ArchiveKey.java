package com.github.gluhov.iotplatform.failedeventsprocessor.domain.model;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Ключ объекта ошибки: {@code <yyyy>/<MM>/<dd>/<topic>-<partition>-<offset>.json}, где тема, раздел и смещение -
 * координаты самой записи failed-events, а дата - её время (timestamp) в UTC. Ключ один для читаемой и нечитаемой
 * записи и не меняется при повторной доставке: после сбоя между записью объекта и подтверждением смещения запись
 * попадает в тот же ключ в любой ветке (FR-008, FR-010, ADR-0014). Символы темы вне {@code [A-Za-z0-9._-]}
 * заменяются на {@code _}, чтобы ключ был безопасен для S3 и путей.
 */
public final class ArchiveKey {

    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy/MM/dd").withZone(ZoneOffset.UTC);

    private ArchiveKey() {
    }

    public static String of(String topic, int partition, long offset, Instant recordTimestamp) {
        return DATE.format(recordTimestamp) + "/" + safe(topic) + "-" + partition + "-" + offset + ".json";
    }

    public static String of(ArchiveCoordinates coordinates) {
        return of(coordinates.topic(), coordinates.partition(), coordinates.offset(), coordinates.recordTimestamp());
    }

    public static String of(UnreadableRecord record) {
        return of(record.topic(), record.partition(), record.offset(), record.recordTimestamp());
    }

    private static String safe(String topic) {
        return topic.replaceAll("[^A-Za-z0-9._-]", "_");
    }
}
