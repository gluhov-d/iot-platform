package com.github.gluhov.iotplatform.failedeventsprocessor.adapter.in.kafka;

import com.github.gluhov.iotplatform.events.avro.ErrorCode;
import com.github.gluhov.iotplatform.events.avro.ErrorType;
import com.github.gluhov.iotplatform.events.avro.FailedEvent;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveDeniedException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveUnavailableException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.ArchiveCoordinates;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.FailedRecord;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.PutOutcome;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.service.ArchiveService;
import com.github.gluhov.iotplatform.failedeventsprocessor.support.MetricsFacade;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.support.Acknowledgment;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FailedEventListenerTest {

    private static final Instant NOW = Instant.parse("2026-11-21T10:15:31.902Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void givenReadableFailedEvent_whenListen_thenArchivedWithCoordinatesAndAcknowledged() {
        ArchiveService archive = mock(ArchiveService.class);
        when(archive.archive(any(FailedRecord.class), any(ArchiveCoordinates.class))).thenReturn(PutOutcome.CREATED);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Acknowledgment ack = mock(Acknowledgment.class);
        FailedEventListener listener = new FailedEventListener(archive, new MetricsFacade(registry), CLOCK);

        Instant recordTime = Instant.parse("2026-11-21T10:15:31.700Z");

        listener.onMessage(new ConsumerRecord<>("failed-events", 1, 7L, recordTime.toEpochMilli(),
                org.apache.kafka.common.record.TimestampType.CREATE_TIME, -1, -1, "events-2-1042", event(),
                new org.apache.kafka.common.header.internals.RecordHeaders(), java.util.Optional.empty()), ack);

        ArgumentCaptor<FailedRecord> record = ArgumentCaptor.forClass(FailedRecord.class);
        ArgumentCaptor<ArchiveCoordinates> coords = ArgumentCaptor.forClass(ArchiveCoordinates.class);
        verify(archive).archive(record.capture(), coords.capture());
        assertThat(record.getValue().sourceService()).isEqualTo("events-collector-service");
        assertThat(record.getValue().sourceTimestamp()).isEqualTo(Instant.parse("2026-11-21T10:15:30.123Z"));
        assertThat(record.getValue().errorCode()).isEqualTo("VALIDATION_FAILED");
        assertThat(record.getValue().errorType()).isEqualTo("PERMANENT");
        assertThat(new String(record.getValue().value(), StandardCharsets.UTF_8)).isEqualTo("payload");
        assertThat(record.getValue().headers()).containsKey("traceparent");
        assertThat(coords.getValue()).isEqualTo(new ArchiveCoordinates("failed-events", 1, 7L, recordTime, NOW));
        verify(ack).acknowledge();
        assertThat(registry.get("failed.events.archived").tag("outcome", "created").counter().count())
                .isEqualTo(1.0);
    }

    @Test
    void givenArchiveUnavailable_whenListen_thenRethrownForRetryAndNotAcknowledged() {
        ArchiveService archive = mock(ArchiveService.class);
        when(archive.archive(any(FailedRecord.class), any(ArchiveCoordinates.class)))
                .thenThrow(new ArchiveUnavailableException("timeout", null));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Acknowledgment ack = mock(Acknowledgment.class);
        FailedEventListener listener = new FailedEventListener(archive, new MetricsFacade(registry), CLOCK);

        assertThatThrownBy(() -> listener.onMessage(
                new ConsumerRecord<>("failed-events", 1, 7L, "k", event()), ack))
                .isInstanceOf(ArchiveUnavailableException.class);

        verify(ack, never()).acknowledge();
        assertThat(registry.get("failed.events.archive.errors").tag("reason", "s3_unavailable")
                .counter().count()).isEqualTo(1.0);
    }

    @Test
    void givenArchiveDenied_whenListen_thenRethrownAndNotAcknowledged() {
        ArchiveService archive = mock(ArchiveService.class);
        when(archive.archive(any(FailedRecord.class), any(ArchiveCoordinates.class)))
                .thenThrow(new ArchiveDeniedException("denied", null));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Acknowledgment ack = mock(Acknowledgment.class);
        FailedEventListener listener = new FailedEventListener(archive, new MetricsFacade(registry), CLOCK);

        assertThatThrownBy(() -> listener.onMessage(
                new ConsumerRecord<>("failed-events", 1, 7L, "k", event()), ack))
                .isInstanceOf(ArchiveDeniedException.class);

        verify(ack, never()).acknowledge();
        assertThat(registry.get("failed.events.archive.errors").tag("reason", "s3_denied")
                .counter().count()).isEqualTo(1.0);
    }

    /**
     * Неразобранная запись (value == null) не архивируется и не подтверждается слушателем: исключение уходит в
     * обработчик ошибок, который решает про unreadable (исходные байты - в DeserializationException).
     */
    @Test
    void givenUnreadableRecord_whenListen_thenDeserializationExceptionNotArchivedNotAcknowledged() {
        ArchiveService archive = mock(ArchiveService.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        FailedEventListener listener = new FailedEventListener(archive,
                new MetricsFacade(new SimpleMeterRegistry()), CLOCK);

        assertThatThrownBy(() -> listener.onMessage(
                new ConsumerRecord<>("failed-events", 1, 8L, "k", null), ack))
                .isInstanceOf(org.springframework.kafka.support.serializer.DeserializationException.class);

        verify(archive, never()).archive(any(FailedRecord.class), any(ArchiveCoordinates.class));
        verify(ack, never()).acknowledge();
    }

    /**
     * Значение - корректный Avro другого типа (не FailedEvent): слушатель сам проверяет тип и бросает ошибку разбора,
     * без ClassCastException при вызове. Архив не вызывается, смещение не подтверждается.
     */
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void givenValueNotFailedEvent_whenListen_thenDeserializationExceptionNotArchivedNotAcknowledged() {
        ArchiveService archive = mock(ArchiveService.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        FailedEventListener listener = new FailedEventListener(archive,
                new MetricsFacade(new SimpleMeterRegistry()), CLOCK);
        ConsumerRecord notFailedEvent = new ConsumerRecord<>("failed-events", 1, 9L, "k",
                "не FailedEvent");

        assertThatThrownBy(() -> listener.onMessage(notFailedEvent, ack))
                .isInstanceOf(org.springframework.kafka.support.serializer.DeserializationException.class);

        verify(archive, never()).archive(any(FailedRecord.class), any(ArchiveCoordinates.class));
        verify(ack, never()).acknowledge();
    }

    private static FailedEvent event() {
        return FailedEvent.newBuilder()
                .setSourceService("events-collector-service")
                .setSourceTopic("events")
                .setSourcePartition(2)
                .setSourceOffset(1042L)
                .setSourceTimestamp(Instant.parse("2026-11-21T10:15:30.123Z"))
                .setFailedAt(Instant.parse("2026-11-21T10:15:31.480Z"))
                .setErrorCode(ErrorCode.VALIDATION_FAILED)
                .setErrorType(ErrorType.PERMANENT)
                .setDescription("InvalidEventException: value must be finite")
                .setTraceId("0af7651916cd43dd8448eb211c80319c")
                .setKey(ByteBuffer.wrap("dev-42".getBytes(StandardCharsets.UTF_8)))
                .setValue(ByteBuffer.wrap("payload".getBytes(StandardCharsets.UTF_8)))
                .setHeaders(Map.of("traceparent", ByteBuffer.wrap(new byte[]{1, 2})))
                .setAttempts(1)
                .setDeviceId("dev-42")
                .build();
    }
}
