package com.github.gluhov.iotplatform.eventscollector.service.adapter.in;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.eventscollector.adapter.in.kafka.KafkaEventListener;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.EventPersistenceException;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.InvalidEventException;
import com.github.gluhov.iotplatform.eventscollector.domain.model.DeviceEventRecord;
import com.github.gluhov.iotplatform.eventscollector.domain.model.OutboxRecord;
import com.github.gluhov.iotplatform.eventscollector.domain.model.OutboxStatus;
import com.github.gluhov.iotplatform.eventscollector.domain.model.TraceRef;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.CollectorMetrics;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.DeviceEventRepository;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.OutboxRepository;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.TraceContextCarrier;
import com.github.gluhov.iotplatform.eventscollector.domain.service.DeviceEventMapper;
import com.github.gluhov.iotplatform.eventscollector.domain.service.DeviceEventValidator;
import com.github.gluhov.iotplatform.eventscollector.domain.service.EventIngestionService;
import io.micrometer.observation.ObservationRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneOffset;

import static com.github.gluhov.iotplatform.eventscollector.data.TestData.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EventIngestionScenarioTest {
    private static final String TOPIC = "events";
    private static final TraceRef INGESTION_TRACE = new TraceRef("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7");

    @Mock
    private DeviceEventRepository deviceEventRepository;
    @Mock
    private OutboxRepository outboxRepository;
    @Mock
    private CollectorMetrics metrics;
    @Mock
    private Acknowledgment ack;
    @Mock
    private TraceContextCarrier traceContext;

    private KafkaEventListener listener;

    @BeforeEach
    void setUp() {
        Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
        EventIngestionService service = new EventIngestionService(
                new DeviceEventValidator(clock), new DeviceEventMapper(),
                deviceEventRepository, outboxRepository, metrics, ObservationRegistry.NOOP, traceContext, clock);
        listener = new KafkaEventListener(service);
        lenient().when(traceContext.current()).thenReturn(INGESTION_TRACE);
    }

    @Test
    @DisplayName("Receive valid event then store event and outbox and acknowledge")
    void givenValidEvent_whenOnEvent_thenStoreAndAcknowledge() {
        DeviceEvent deviceEvent = validEvent();
        listener.onEvent(record(deviceEvent), ack);

        InOrder order = inOrder(deviceEventRepository, outboxRepository, ack);
        ArgumentCaptor<DeviceEventRecord> deviceEventRow = ArgumentCaptor.forClass(DeviceEventRecord.class);
        ArgumentCaptor<OutboxRecord> outboxRecordRow = ArgumentCaptor.forClass(OutboxRecord.class);
        order.verify(deviceEventRepository).insert(deviceEventRow.capture());
        order.verify(outboxRepository).insert(outboxRecordRow.capture());
        order.verify(ack).acknowledge();

        assertThat(deviceEventRow.getValue().eventId()).isEqualTo(EVENT_ID);
        assertThat(deviceEventRow.getValue().eventDate()).isEqualTo(LocalDate.of(2026, 10, 1));
        assertThat(deviceEventRow.getValue().ingestedAt()).isEqualTo(NOW);

        OutboxRecord outboxRecord = outboxRecordRow.getValue();
        assertThat(outboxRecord.status()).isEqualTo(OutboxStatus.NEW);
        assertThat(outboxRecord.stateVersion()).isEqualTo(OutboxRecord.INITIAL_VERSION);
        assertThat(outboxRecord.attempts()).isZero();
        assertThat(outboxRecord.sourceTimestampMs()).isEqualTo(deviceEvent.getTimestampMs());
        assertThat(outboxRecord.traceRef()).as("Ingestion span saved for publication").isEqualTo(INGESTION_TRACE);

        verify(metrics).eventProcessed();
        verify(metrics).outboxCandidateCreated();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    @DisplayName("Receive event with blank eventId then reject without ack")
    void givenBlankEventId_whenOnEvent_thenRejectWithoutAck(String eventId) {
        assertRejected(event(eventId, DEVICE_ID, NOW.toEpochMilli()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    @DisplayName("Receive event with blank deviceId then reject without ack")
    void givenBlankDeviceId_whenOnEvent_thenRejectWithoutAck(String deviceId) {
        assertRejected(event(EVENT_ID, deviceId, NOW.toEpochMilli()));
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, -1L, Long.MAX_VALUE})
    @DisplayName("Receive event with invalid timestamp then reject without ack")
    void givenInvalidTimestamp_whenOnEvent_thenRejectWithoutAck(long timestampMs) {
        assertRejected(event(EVENT_ID, DEVICE_ID, timestampMs));
    }

    @Test
    @DisplayName("device_events insert fails then no outbox and no ack")
    void givenEventInsertFailure_whenOnEvent_thenNoAck() {
        doThrow(new IllegalStateException("clickhouse down")).when(deviceEventRepository).insert(any());

        assertThatThrownBy(() -> listener.onEvent(record(validEvent()), ack))
                .isInstanceOf(EventPersistenceException.class);

        verify(outboxRepository, never()).insert(any());
        verify(ack, never()).acknowledge();
        verify(metrics).eventPersistFailed();
        verify(metrics, never()).eventProcessed();
    }

    @Test
    @DisplayName("device_outbox insert fails then no ack")
    void givenOutboxInsertFailure_whenOnEvent_thenNoAck() {
        doThrow(new IllegalStateException("clickhouse down")).when(outboxRepository).insert(any());

        assertThatThrownBy(() -> listener.onEvent(record(validEvent()), ack))
                .isInstanceOf(EventPersistenceException.class);

        verify(deviceEventRepository).insert(any());
        verify(ack, never()).acknowledge();
        verify(metrics).eventPersistFailed();
        verify(metrics, never()).outboxCandidateCreated();
    }

    @Test
    @DisplayName("Message key differs deviceId then only logged and acknowledged")
    void givenKeyMismatch_whenOnEvent_thenAcknowledge() {
        ConsumerRecord<String, DeviceEvent> record = new ConsumerRecord<>(TOPIC, 0, 5L, "other-key", validEvent());

        listener.onEvent(record, ack);

        verify(ack).acknowledge();
    }

    private static ConsumerRecord<String, DeviceEvent> record(DeviceEvent deviceEvent) {
        return new ConsumerRecord<>(TOPIC, 1, 42L, deviceEvent.getDeviceId(),deviceEvent);
    }

    private void assertRejected(DeviceEvent deviceEvent) {
        assertThatThrownBy(() -> listener.onEvent(record(deviceEvent), ack))
                .isInstanceOf(InvalidEventException.class);
        verifyNoInteractions(deviceEventRepository, outboxRepository);
        verify(ack, never()).acknowledge();
    }
}
