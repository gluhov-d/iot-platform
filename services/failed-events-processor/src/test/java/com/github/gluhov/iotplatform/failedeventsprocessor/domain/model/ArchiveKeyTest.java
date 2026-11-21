package com.github.gluhov.iotplatform.failedeventsprocessor.domain.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class ArchiveKeyTest {

    @Test
    void givenFailedEvent_whenKey_thenFromSourceCoordinatesAndSourceTimestampUtc() {
        // 2026-11-21T23:30:00Z: дата в ключе - UTC, а не местная
        FailedRecord record = record("devices", 1, 1042L, Instant.parse("2026-11-21T23:30:00Z"),
                Instant.parse("2026-11-22T00:05:00Z"), null);

        assertThat(ArchiveKey.of(record))
                .isEqualTo("device-collector-service/2026/11/21/STORAGE_UNAVAILABLE/devices-1-1042.json");
    }

    @Test
    void givenTwoPublicationsOfSameSource_whenKey_thenSameKey() {
        // Повторная публикация: другое время ошибки и другой traceId, те же координаты и время источника
        Instant sourceTs = Instant.parse("2026-11-21T10:15:30.123Z");
        FailedRecord first = record("events", 2, 1042L, sourceTs, Instant.parse("2026-11-21T10:15:31Z"), "dev-1");
        FailedRecord second = record("events", 2, 1042L, sourceTs, Instant.parse("2026-11-21T12:00:00Z"), null);

        assertThat(ArchiveKey.of(second)).isEqualTo(ArchiveKey.of(first));
    }

    @Test
    void givenNoDeviceId_whenKey_thenSameLayoutWithoutDevice() {
        FailedRecord record = record("events", 0, 7L, Instant.parse("2026-11-21T10:15:30Z"),
                Instant.parse("2026-11-21T10:15:31Z"), null);

        assertThat(ArchiveKey.of(record))
                .isEqualTo("device-collector-service/2026/11/21/STORAGE_UNAVAILABLE/events-0-7.json");
    }

    @Test
    void givenSpecialCharsInTopic_whenKey_thenOnlySafeCharsInKey() {
        FailedRecord record = record("dev/ices r#1", 0, 7L, Instant.parse("2026-11-21T10:15:30Z"),
                Instant.parse("2026-11-21T10:15:31Z"), null);

        String key = ArchiveKey.of(record);

        assertThat(key).endsWith("/dev_ices_r_1-0-7.json");
        assertThat(key.substring(key.lastIndexOf('/') + 1)).matches("[A-Za-z0-9._-]+\\.json");
    }

    @Test
    void givenUnreadableRecord_whenKey_thenLayoutByRecordTimeUtc() {
        // Дата - время записи в failed-events (UTC), координаты - failed-events
        UnreadableRecord record = new UnreadableRecord("failed-events", 1, 8L,
                Instant.parse("2026-11-21T23:30:00Z"), "SerializationException: Unknown magic byte!",
                null, new byte[]{1, 2, 3}, java.util.Map.of());

        assertThat(ArchiveKey.unreadable(record))
                .isEqualTo("unreadable/2026/11/21/failed-events-1-8.json");
    }

    private static FailedRecord record(String topic, int partition, long offset, Instant sourceTs,
                                       Instant failedAt, String deviceId) {
        return new FailedRecord("device-collector-service", topic, partition, offset, sourceTs, failedAt,
                "STORAGE_UNAVAILABLE", "TRANSIENT", "EventPersistenceException: boom", null,
                null, null, java.util.Map.of(), 3, deviceId);
    }
}
