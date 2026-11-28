package com.github.gluhov.iotplatform.failedeventsprocessor.domain.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ArchiveKeyTest {

    private static final Instant RECORD_TIME = Instant.parse("2026-11-21T23:30:00Z");

    @Test
    void givenRecord_whenKey_thenFromFailedEventsPartitionOffsetAndRecordTimestampUtc() {
        // 2026-11-21T23:30:00Z: дата в ключе - UTC, а не местная
        ArchiveCoordinates coordinates = new ArchiveCoordinates("failed-events", 1, 1042L, RECORD_TIME,
                Instant.parse("2026-11-22T00:05:00Z"));

        assertThat(ArchiveKey.of(coordinates)).isEqualTo("2026/11/21/failed-events-1-1042.json");
    }

    @Test
    void givenUnreadableAndReadableOfSameRecord_whenKey_thenSameKey() {
        // Одна запись failed-events: сначала нечитаемая (схема не найдена), после восстановления схемы - разобранная
        UnreadableRecord unreadable = new UnreadableRecord("failed-events", 1, 8L, RECORD_TIME,
                "SerializationException: Unknown magic byte!", null, new byte[]{1, 2, 3}, List.of());
        ArchiveCoordinates readable = new ArchiveCoordinates("failed-events", 1, 8L, RECORD_TIME,
                Instant.parse("2026-11-22T09:00:00Z"));

        assertThat(ArchiveKey.of(unreadable)).isEqualTo(ArchiveKey.of(readable))
                .isEqualTo("2026/11/21/failed-events-1-8.json");
    }

    @Test
    void givenRedelivery_whenKey_thenSameKeyAndSameDate() {
        // Повторная доставка той же записи в другое время (архивация позже, чем в первый раз): ключ не меняется
        ArchiveCoordinates first = new ArchiveCoordinates("failed-events", 2, 7L, RECORD_TIME,
                Instant.parse("2026-11-21T23:30:05Z"));
        ArchiveCoordinates afterMidnight = new ArchiveCoordinates("failed-events", 2, 7L, RECORD_TIME,
                Instant.parse("2026-11-22T03:00:00Z"));

        assertThat(ArchiveKey.of(afterMidnight)).isEqualTo(ArchiveKey.of(first));
    }

    @Test
    void givenDifferentRecords_whenKey_thenDifferentKeys() {
        ArchiveCoordinates first = new ArchiveCoordinates("failed-events", 0, 7L, RECORD_TIME, RECORD_TIME);
        ArchiveCoordinates otherOffset = new ArchiveCoordinates("failed-events", 0, 8L, RECORD_TIME, RECORD_TIME);
        ArchiveCoordinates otherPartition = new ArchiveCoordinates("failed-events", 1, 7L, RECORD_TIME, RECORD_TIME);

        assertThat(ArchiveKey.of(otherOffset)).isNotEqualTo(ArchiveKey.of(first));
        assertThat(ArchiveKey.of(otherPartition)).isNotEqualTo(ArchiveKey.of(first));
    }

    @Test
    void givenSpecialCharsInTopic_whenKey_thenOnlySafeCharsInKey() {
        String key = ArchiveKey.of("dev/ices r#1", 0, 7L, Instant.parse("2026-11-21T10:15:30Z"));

        assertThat(key).isEqualTo("2026/11/21/dev_ices_r_1-0-7.json");
        assertThat(key.substring(key.lastIndexOf('/') + 1)).matches("[A-Za-z0-9._-]+\\.json");
    }
}
