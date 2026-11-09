package com.github.gluhov.iotplatform.eventscollector.service.domain.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceSnapshot;
import com.github.gluhov.iotplatform.eventscollector.domain.service.DeviceFingerprint;
import com.github.gluhov.iotplatform.events.avro.DeviceStatus;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.PublisherUnavailableException;
import com.github.gluhov.iotplatform.eventscollector.domain.model.*;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.*;
import com.github.gluhov.iotplatform.eventscollector.domain.service.DeviceSnapshotJson;
import com.github.gluhov.iotplatform.eventscollector.domain.service.OutboxPublishingService;
import com.github.gluhov.iotplatform.eventscollector.support.LogCapture;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.github.gluhov.iotplatform.eventscollector.data.TestData.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxPublishingServiceTest {
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final String TOKEN = "owner-token";
    private static final int BATCH_SIZE = 100;
    private static final int MAX_ATTEMPTS = 10;

    @Mock
    private PublisherLease lease;
    @Mock
    private OutboxRepository outboxRepository;
    @Mock
    private PublishedDeviceStore publishedDeviceStore;
    @Mock
    private DeviceInfoPublisher publisher;
    @Mock
    private PublicationAuditRepository publicationAuditRepository;
    @Mock
    private CollectorMetrics metrics;
    @Mock
    private TraceContextCarrier traceContextCarrier;

    private OutboxPublishingService service;
    private OutboxRecord pending;

    @BeforeEach
    void setUp() {
        service = new OutboxPublishingService(lease, outboxRepository, publishedDeviceStore, publisher, publicationAuditRepository, metrics,
                ObservationRegistry.NOOP, traceContextCarrier, new OutboxPolicy(BATCH_SIZE, MAX_ATTEMPTS, TTL), Clock.fixed(NOW, ZoneOffset.UTC));
        pending = OutboxRecord.initial(EVENT_ID, DEVICE_ID, 1_000L, NOW.minusSeconds(5), TraceRef.NONE);

        // Без трассировки: работа просто выполняется (doAnswer - чтобы при настройке не вызывался сам ответ)
        lenient().doAnswer(call -> ((Supplier<?>) call.getArgument(1)).get())
                .when(traceContextCarrier).continueTrace(any(), any());

        lenient().when(lease.tryAcquire(TTL)).thenReturn(Optional.of(TOKEN));
        lenient().when(lease.release(TOKEN)).thenReturn(true);
        lenient().when(outboxRepository.findPending(MAX_ATTEMPTS, BATCH_SIZE)).thenReturn(List.of(pending));
        lenient().when(lease.extend(TOKEN, TTL)).thenReturn(true);
    }

    @Test
    @DisplayName("Publish new device then send, mark published and append SENT version")
    void givenNewDevice_whenPublishBatch_thenPublishMarkAndSent() {
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(false);

        BatchResult result = service.publishPendingBatch();

        InOrder order = inOrder(publisher, publishedDeviceStore, outboxRepository, lease);
        ArgumentCaptor<DeviceInfo> info = ArgumentCaptor.forClass(DeviceInfo.class);
        order.verify(publisher).publish(info.capture());
        order.verify(publishedDeviceStore).markPublished(DEVICE_ID);
        OutboxRecord sent = captureInsert(order);
        order.verify(lease).release(TOKEN);

        assertThat(sent.status()).isEqualTo(OutboxStatus.SENT);
        assertThat(sent.stateVersion()).isEqualTo(pending.stateVersion() + 1);
        assertThat(sent.updatedAt()).isEqualTo(NOW);
        assertThat(sent.createdAt()).isEqualTo(pending.createdAt());
        assertThat(result.published()).isEqualTo(1);
        verify(metrics).publishSucceeded();
        // Телеметрия без снимка у нового устройства: CREATED без модели устройства
        assertThat(info.getValue().getDeviceId()).isEqualTo(DEVICE_ID);
        assertThat(info.getValue().getEventType()).isEqualTo(DeviceUpsertEventType.CREATED);
        assertThat(info.getValue().getEmittedAtMs()).isEqualTo(NOW.toEpochMilli());
        assertThat(info.getValue().getEventId()).isEqualTo(EVENT_ID);
        assertThat(info.getValue().getEventTimeMs()).isEqualTo(1_000L);
        assertThat(info.getValue().getDevice()).isNull();
    }

    @Test
    @DisplayName("New device with snapshot then CREATED with the full device model")
    void givenNewDeviceWithSnapshot_whenPublishBatch_thenCreatedWithSnapshot() {
        DeviceSnapshot snapshot = snapshot(DEVICE_ID, DeviceStatus.ACTIVE);
        givenPending(DeviceSnapshotJson.toJson(snapshot));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(false);

        service.publishPendingBatch();

        DeviceInfo info = capturePublished();
        assertThat(info.getEventType()).isEqualTo(DeviceUpsertEventType.CREATED);
        assertThat(info.getDevice()).isEqualTo(snapshot);
    }

    @Test
    @DisplayName("Known device with snapshot then UPDATED is published, not skipped")
    void givenKnownDeviceWithSnapshot_whenPublishBatch_thenUpdated() {
        DeviceSnapshot snapshot = snapshot(DEVICE_ID, DeviceStatus.MAINTENANCE);
        givenPending(DeviceSnapshotJson.toJson(snapshot));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(true);

        BatchResult result = service.publishPendingBatch();

        DeviceInfo info = capturePublished();
        assertThat(info.getEventType()).isEqualTo(DeviceUpsertEventType.UPDATED);
        assertThat(info.getDevice().getStatus()).isEqualTo(DeviceStatus.MAINTENANCE);
        assertThat(result.published()).isEqualTo(1);
        assertThat(result.skipped()).isZero();
        assertThat(captureInsert(inOrder(outboxRepository)).status()).isEqualTo(OutboxStatus.SENT);
    }

    @Test
    @DisplayName("Snapshot with status DELETED then DELETED for known and for new device")
    void givenDeletedSnapshot_whenPublishBatch_thenDeleted() {
        givenPending(DeviceSnapshotJson.toJson(snapshot(DEVICE_ID, DeviceStatus.DELETED)));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(true, false);

        service.publishPendingBatch();
        service.publishPendingBatch();

        ArgumentCaptor<DeviceInfo> info = ArgumentCaptor.forClass(DeviceInfo.class);
        verify(publisher, times(2)).publish(info.capture());
        assertThat(info.getAllValues()).extracting(DeviceInfo::getEventType)
                .containsExactly(DeviceUpsertEventType.DELETED, DeviceUpsertEventType.DELETED);
    }

    @Test
    @DisplayName("Kafka send fails then FAILED log carries the change type in MDC")
    void givenKafkaFailure_whenPublishBatch_thenFailedLogHasEventType() {
        givenPending(DeviceSnapshotJson.toJson(snapshot(DEVICE_ID, DeviceStatus.ACTIVE)));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(true);
        doThrow(new IllegalStateException("broker unavailable")).when(publisher).publish(forDevice(DEVICE_ID));

        ILoggingEvent warn;
        try (LogCapture logs = LogCapture.of(OutboxPublishingService.class)) {
            service.publishPendingBatch();
            warn = logs.single(Level.WARN, "DeviceInfo publish failed: IllegalStateException: broker unavailable");
        }

        assertThat(warn.getMDCPropertyMap())
                .containsEntry("eventType", "UPDATED")
                .containsEntry("status", "FAILED")
                .containsEntry("eventId", EVENT_ID);
    }

    @Test
    @DisplayName("Publish already published device then skip with SENT version")
    void givenPublishedDevice_whenPublishBatch_thenSkipWithSentVersion() {
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(true);

        BatchResult result = service.publishPendingBatch();

        verify(publisher, never()).publish(any());
        verify(publishedDeviceStore, never()).markPublished(anyString());
        OutboxRecord sent = captureInsert(inOrder(outboxRepository));
        assertThat(sent.status()).isEqualTo(OutboxStatus.SENT);
        assertThat(sent.stateVersion()).isEqualTo(2);
        assertThat(sent.attempts()).isZero();
        assertThat(result.skipped()).isEqualTo(1);
        verify(metrics).publishSkipped();
    }

    @Test
    @DisplayName("Kafka send fails then append FAILED version with incremented attempts")
    void givenKafkaFailure_whenPublishBatch_thenFailedVersionWithIncrementedAttempts() {
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(false);
        doThrow(new IllegalStateException("broker unavailable")).when(publisher).publish(forDevice(DEVICE_ID));

        BatchResult result = service.publishPendingBatch();

        verify(publishedDeviceStore, never()).markPublished(anyString());
        OutboxRecord failed = captureInsert(inOrder(outboxRepository));
        assertThat(failed.status()).isEqualTo(OutboxStatus.FAILED);
        assertThat(failed.attempts()).isEqualTo(pending.attempts() + 1);
        assertThat(failed.stateVersion()).isEqualTo(pending.stateVersion() + 1);
        assertThat(failed.lastError()).contains("broker unavailable");
        assertThat(result.failed()).isEqualTo(1);
        verify(metrics).publishFailed();
    }

    @Test
    @DisplayName("Retry failed record then version chain continues")
    void givenFailedRecord_whenPublishBatch_thenVersionChainContinues() {
        OutboxRecord failedOnce = pending.failed(NOW.minusSeconds(1), "timeout");
        when(outboxRepository.findPending(MAX_ATTEMPTS, BATCH_SIZE)).thenReturn(List.of(failedOnce));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(false);

        service.publishPendingBatch();

        OutboxRecord sent = captureInsert(inOrder(outboxRepository));
        assertThat(sent.status()).isEqualTo(OutboxStatus.SENT);
        assertThat(sent.stateVersion()).isEqualTo(3);
        assertThat(sent.attempts()).isEqualTo(1);
        assertThat(sent.lastError()).isEmpty();
    }

    @Test
    @DisplayName("Lease not acquired then nothing published")
    void givenLeaseNotAcquired_whenPublishBatch_thenNothingPublished() {
        when(lease.tryAcquire(TTL)).thenReturn(Optional.empty());

        BatchResult result = service.publishPendingBatch();

        assertThat(result.leaseAcquired()).isFalse();
        verify(outboxRepository, never()).findPending(anyInt(), anyInt());
        verify(publisher, never()).publish(any());
        verify(lease, never()).release(anyString());
    }

    @Test
    @DisplayName("Selection fails then lease released and error propagated")
    void givenSelectionFailure_whenPublishBatch_thenLeaseReleased() {
        when(outboxRepository.findPending(MAX_ATTEMPTS, BATCH_SIZE)).thenThrow(new IllegalStateException("clickhouse down"));

        assertThatThrownBy(() -> service.publishPendingBatch()).isInstanceOf(IllegalStateException.class);

        verify(lease).release(TOKEN);
    }

    @Test
    @DisplayName("Storage error on one record then batch continues")
    void givenStorageErrorOnOneRecord_whenPublishBatch_thenBatchContinues() {
        OutboxRecord second = OutboxRecord.initial("evt-2", "dev-2", 2_000L, NOW, TraceRef.NONE);
        when(outboxRepository.findPending(MAX_ATTEMPTS, BATCH_SIZE)).thenReturn(List.of(pending, second));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenThrow(new IllegalStateException("redis down"));
        when(publishedDeviceStore.isPublished("dev-2")).thenReturn(false);

        BatchResult result = service.publishPendingBatch();

        assertThat(result.errors()).isEqualTo(1);
        assertThat(result.published()).isEqualTo(1);
        verify(publisher).publish(forDevice("dev-2"));
    }

    @Test
    @DisplayName("Redis fails during publish then error log and audit written inside event MDC and ingestion trace")
    void givenRedisFailure_whenPublishBatch_thenErrorLoggedAndAuditedInsideEventScope() {
        TraceRef ingestion = new TraceRef("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7");
        OutboxRecord traced = OutboxRecord.initial(EVENT_ID, DEVICE_ID, 1_000L, NOW, ingestion);
        when(outboxRepository.findPending(MAX_ATTEMPTS, BATCH_SIZE)).thenReturn(List.of(traced));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenThrow(new IllegalStateException("redis down"));
        // Как настоящий перенос трейса: пока выполняется работа, traceId приема есть в MDC
        doAnswer(call -> {
            try (var ignored = MDC.putCloseable("traceId", ingestion.traceId())) {
                return ((Supplier<?>) call.getArgument(1)).get();
            }
        }).when(traceContextCarrier).continueTrace(eq(ingestion), any());
        AtomicReference<Map<String, String>> mdcAtAudit = new AtomicReference<>();
        doAnswer(call -> {
            mdcAtAudit.set(MDC.getCopyOfContextMap());
            return null;
        }).when(publicationAuditRepository).insert(any());

        BatchResult result;
        ILoggingEvent error;
        try (LogCapture logs = LogCapture.of(OutboxPublishingService.class)) {
            result = service.publishPendingBatch();
            error = logs.single(Level.ERROR, "Outbox record not processed, will be selected again");
        }

        assertThat(result.errors()).isEqualTo(1);
        assertThat(error.getMDCPropertyMap())
                .containsEntry("eventId", EVENT_ID)
                .containsEntry("deviceId", DEVICE_ID)
                .containsEntry("traceId", ingestion.traceId())
                .containsEntry("status", "ERROR")
                .containsEntry("errorClass", IllegalStateException.class.getName())
                .containsEntry("errorMessage", "redis down");
        assertThat(error.getThrowableProxy().getMessage()).isEqualTo("redis down");
        assertThat(captureAudit().outcome()).isEqualTo(PublishOutcome.ERROR);
        assertThat(mdcAtAudit.get()).as("audit written inside event MDC and ingestion trace")
                .containsEntry("eventId", EVENT_ID)
                .containsEntry("deviceId", DEVICE_ID)
                .containsEntry("traceId", ingestion.traceId());
    }

    @Test
    @DisplayName("Iteration finished then pending count refreshed")
    void givenIteration_whenPublishBatch_thenPendingCountRefreshed() {
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(true);
        when(outboxRepository.countPending(MAX_ATTEMPTS)).thenReturn(7L);

        service.publishPendingBatch();

        verify(metrics).pendingCount(7L);
    }

    @Test
    @DisplayName("Lease lost before release then batch result kept")
    void givenLostLease_whenPublishBatch_thenResultKept() {
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(true);
        when(lease.release(TOKEN)).thenReturn(false);

        BatchResult result = service.publishPendingBatch();

        assertThat(result.skipped()).isEqualTo(1);
    }

    @Test
    @DisplayName("Batch of several records then lease extended before each record and each outbox write")
    void givenSeveralRecords_whenPublishBatch_thenLeaseExtendedBeforeEachRecordAndWrite() {
        OutboxRecord second = OutboxRecord.initial("evt-2", "dev-2", 2_000L, NOW, TraceRef.NONE);
        OutboxRecord third = OutboxRecord.initial("evt-3", "dev-3", 3_000L, NOW, TraceRef.NONE);
        when(outboxRepository.findPending(MAX_ATTEMPTS, BATCH_SIZE)).thenReturn(List.of(pending, second, third));
        when(publishedDeviceStore.isPublished(anyString())).thenReturn(false);

        BatchResult result = service.publishPendingBatch();

        InOrder order = inOrder(outboxRepository, publisher, lease);
        order.verify(outboxRepository).findPending(MAX_ATTEMPTS, BATCH_SIZE);
        for (String deviceId : List.of(DEVICE_ID, "dev-2", "dev-3")) {
            order.verify(lease).extend(TOKEN, TTL);
            order.verify(publisher).publish(forDevice(deviceId));
            order.verify(lease).extend(TOKEN, TTL);
            order.verify(outboxRepository).insert(argThat(r -> deviceId.equals(r.deviceId())));
        }
        order.verify(lease).release(TOKEN);
        verify(lease, times(6)).extend(TOKEN, TTL);
        assertThat(result.published()).isEqualTo(3);
        assertThat(result.leaseLost()).isFalse();
    }

    @Test
    @DisplayName("Lease lost during batch then remaining records not published")
    void givenLeaseLostMidBatch_whenPublishBatch_thenBatchStops() {
        OutboxRecord second = OutboxRecord.initial("evt-2", "dev-2", 2_000L, NOW, TraceRef.NONE);
        when(outboxRepository.findPending(MAX_ATTEMPTS, BATCH_SIZE)).thenReturn(List.of(pending, second));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(false);
        // Первая запись: перед обработкой и перед SENT - аренда наша; перед второй записью - уже нет
        when(lease.extend(TOKEN, TTL)).thenReturn(true, true, false);
        when(lease.release(TOKEN)).thenReturn(false);

        BatchResult result = service.publishPendingBatch();

        verify(publisher).publish(forDevice(DEVICE_ID));
        verify(outboxRepository).insert(argThat(r -> EVENT_ID.equals(r.eventId())));
        verify(publisher, never()).publish(forDevice("dev-2"));
        verify(outboxRepository, never()).insert(argThat(r -> "evt-2".equals(r.eventId())));
        verify(lease).release(TOKEN);
        assertThat(result.leaseLost()).isTrue();
        assertThat(result.selected()).isEqualTo(2);
        assertThat(result.published()).isEqualTo(1);
    }

    @Test
    @DisplayName("Lease lost during selection then first record not published")
    void givenLeaseLostDuringSelection_whenPublishBatch_thenNothingPublished() {
        when(lease.extend(TOKEN, TTL)).thenReturn(false);

        BatchResult result = service.publishPendingBatch();

        verify(publishedDeviceStore, never()).isPublished(anyString());
        verify(publisher, never()).publish(any());
        verify(outboxRepository, never()).insert(any());
        verify(publicationAuditRepository, never()).insert(any());
        verify(lease).release(TOKEN);
        assertThat(result.leaseLost()).isTrue();
        assertThat(result.selected()).isEqualTo(1);
        assertThat(result.published()).isZero();
    }

    @Test
    @DisplayName("Lease lost during Kafka send then no SENT version written")
    void givenLeaseLostDuringSend_whenPublishBatch_thenNoOutboxVersion() {
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(false);
        when(lease.extend(TOKEN, TTL)).thenReturn(true, false);

        BatchResult result = service.publishPendingBatch();

        // Kafka подтвердила отправку - устройство отмечено; SENT запишет новый владелец (пропуск по devices:published)
        verify(publishedDeviceStore).markPublished(DEVICE_ID);
        verify(outboxRepository, never()).insert(any());
        verify(publicationAuditRepository, never()).insert(any());
        verify(metrics, never()).publishSucceeded();
        assertThat(result.leaseLost()).isTrue();
        assertThat(result.published()).isZero();
        assertThat(result.errors()).isZero();
    }

    @Test
    @DisplayName("Lease lost after Kafka failure then no FAILED version written")
    void givenLeaseLostAfterKafkaFailure_whenPublishBatch_thenNoFailedVersion() {
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(false);
        doThrow(new IllegalStateException("broker unavailable")).when(publisher).publish(forDevice(DEVICE_ID));
        when(lease.extend(TOKEN, TTL)).thenReturn(true, false);

        BatchResult result = service.publishPendingBatch();

        verify(outboxRepository, never()).insert(any());
        verify(metrics, never()).publishFailed();
        assertThat(result.leaseLost()).isTrue();
        assertThat(result.failed()).isZero();
    }

    @Test
    @DisplayName("Redis fails on lease extend then batch stops as with lost lease")
    void givenRedisFailureOnExtend_whenPublishBatch_thenBatchStops() {
        when(lease.extend(TOKEN, TTL)).thenThrow(new IllegalStateException("redis down"));

        BatchResult result = service.publishPendingBatch();

        verify(publisher, never()).publish(any());
        verify(outboxRepository, never()).insert(any());
        verify(lease).release(TOKEN);
        assertThat(result.leaseLost()).isTrue();
    }

    @Test
    @DisplayName("Record with ingestion trace then published inside that trace")
    void givenTracedRecord_whenPublishBatch_thenPublishedInsideIngestionTrace() {
        TraceRef ingestion = new TraceRef("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7");
        OutboxRecord traced = OutboxRecord.initial(EVENT_ID, DEVICE_ID, 1_000L, NOW, ingestion);
        when(outboxRepository.findPending(MAX_ATTEMPTS, BATCH_SIZE)).thenReturn(List.of(traced));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(false);
        AtomicReference<TraceRef> activeTrace = new AtomicReference<>(TraceRef.NONE);
        doAnswer(call -> {
            activeTrace.set(call.getArgument(0));
            try {
                return ((Supplier<?>) call.getArgument(1)).get();
            } finally {
                activeTrace.set(TraceRef.NONE);
            }
        }).when(traceContextCarrier).continueTrace(eq(ingestion), any());
        AtomicReference<TraceRef> traceAtSend = new AtomicReference<>();
        doAnswer(call -> {
            traceAtSend.set(activeTrace.get());
            return null;
        }).when(publisher).publish(forDevice(DEVICE_ID));

        service.publishPendingBatch();

        assertThat(traceAtSend.get()).as("send runs inside ingestion trace").isEqualTo(ingestion);
        assertThat(captureInsert(inOrder(outboxRepository)).traceRef()).as("SENT version keeps trace").isEqualTo(ingestion);
    }

    @Test
    @DisplayName("Publish retried record then audit PUBLISHED with attempt number")
    void givenRetriedRecord_whenPublishBatch_thenAuditPublishedWithAttemptNumber() {
        OutboxRecord failedOnce = pending.failed(NOW.minusSeconds(1), "timeout");
        when(outboxRepository.findPending(MAX_ATTEMPTS, BATCH_SIZE)).thenReturn(List.of(failedOnce));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(false);

        service.publishPendingBatch();

        assertThat(captureAudit()).isEqualTo(
                new PublicationAuditRecord(EVENT_ID, DEVICE_ID, PublishOutcome.PUBLISHED, 2, "", NOW));
    }

    @Test
    @DisplayName("Publish already published device then audit SKIPPED")
    void givenPublishedDevice_whenPublishBatch_thenAuditSkipped() {
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(true);

        service.publishPendingBatch();

        assertThat(captureAudit().outcome()).isEqualTo(PublishOutcome.SKIPPED);
    }

    @Test
    @DisplayName("Kafka send fails then audit FAILED with error text")
    void givenKafkaFailure_whenPublishBatch_thenAuditFailedWithError() {
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(false);
        doThrow(new IllegalStateException("broker unavailable")).when(publisher).publish(forDevice(DEVICE_ID));

        service.publishPendingBatch();

        PublicationAuditRecord audit = captureAudit();
        assertThat(audit.outcome()).isEqualTo(PublishOutcome.FAILED);
        assertThat(audit.attempt()).isEqualTo(1);
        assertThat(audit.error()).contains("broker unavailable");
    }

    @Test
    @DisplayName("Audit write fails then publication still counted as done")
    void givenAuditFailure_whenPublishBatch_thenPublicationKept() {
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(false);
        doThrow(new IllegalStateException("clickhouse down")).when(publicationAuditRepository).insert(any());

        BatchResult result = service.publishPendingBatch();

        assertThat(result.published()).isEqualTo(1);
        assertThat(result.errors()).isZero();
        verify(publishedDeviceStore).markPublished(DEVICE_ID);
        assertThat(captureInsert(inOrder(outboxRepository)).status()).isEqualTo(OutboxStatus.SENT);
        verify(metrics).publishSucceeded();
    }

    @Test
    @DisplayName("Kafka unavailable then DEFERRED: no FAILED version, attempts not spent, no audit, batch stopped")
    void givenPublisherUnavailable_whenPublishBatch_thenDeferredWithoutAttempt() {
        OutboxRecord second = OutboxRecord.initial("evt-2", "dev-2", 2_000L, NOW, TraceRef.NONE);
        when(outboxRepository.findPending(MAX_ATTEMPTS, BATCH_SIZE)).thenReturn(List.of(pending, second));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(false);
        doThrow(new PublisherUnavailableException("No Kafka acknowledgement within PT5S", null))
                .when(publisher).publish(forDevice(DEVICE_ID));

        BatchResult result = service.publishPendingBatch();

        verify(outboxRepository, never()).insert(any());
        verify(publicationAuditRepository, never()).insert(any());
        verify(publisher, never()).publish(forDevice("dev-2"));
        assertThat(result.deferred()).isEqualTo(2);
        assertThat(result.failed()).isZero();
    }

    @Test
    @DisplayName("Record of a device failed then later records of the same device wait for the next iteration")
    void givenFailedRecord_whenSameDeviceLaterInBatch_thenDeferred() {
        OutboxRecord later = OutboxRecord.initial("evt-2", DEVICE_ID, 2_000L, NOW, TraceRef.NONE);
        OutboxRecord other = OutboxRecord.initial("evt-3", "dev-3", 3_000L, NOW, TraceRef.NONE);
        when(outboxRepository.findPending(MAX_ATTEMPTS, BATCH_SIZE)).thenReturn(List.of(pending, later, other));
        when(publishedDeviceStore.isPublished(anyString())).thenReturn(false);
        doThrow(new IllegalStateException("record too large")).when(publisher).publish(forDevice(DEVICE_ID));

        BatchResult result = service.publishPendingBatch();

        verify(publisher, times(1)).publish(forDevice(DEVICE_ID));
        verify(publisher).publish(forDevice("dev-3"));
        assertThat(result.failed()).isEqualTo(1);
        assertThat(result.deferred()).isEqualTo(1);
        assertThat(result.published()).isEqualTo(1);
    }

    @Test
    @DisplayName("Snapshot older than the published one then SKIPPED: the topic keeps the latest device state")
    void givenOlderSnapshot_whenPublishBatch_thenSkipped() {
        givenPending(DeviceSnapshotJson.toJson(snapshot(DEVICE_ID, DeviceStatus.ACTIVE)));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(true);
        when(publishedDeviceStore.lastSnapshotAt(DEVICE_ID)).thenReturn(OptionalLong.of(5_000L));

        BatchResult result = service.publishPendingBatch();

        verify(publisher, never()).publish(any());
        assertThat(captureInsert(inOrder(outboxRepository)).status()).isEqualTo(OutboxStatus.SENT);
        assertThat(result.skipped()).isEqualTo(1);
    }

    @Test
    @DisplayName("Snapshot published then its event time remembered for the device")
    void givenSnapshotPublished_whenPublishBatch_thenSnapshotTimeStored() {
        givenPending(DeviceSnapshotJson.toJson(snapshot(DEVICE_ID, DeviceStatus.ACTIVE)));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(true);
        when(publishedDeviceStore.lastSnapshotAt(DEVICE_ID)).thenReturn(OptionalLong.of(500L));

        service.publishPendingBatch();

        verify(publishedDeviceStore).markSnapshotAt(DEVICE_ID, 1_000L);
    }

    @Test
    @DisplayName("Device data unchanged (same fingerprint) then SKIPPED, event time remembered, nothing published")
    void givenSameFingerprint_whenPublishBatch_thenSkippedAndSnapshotTimeAdvanced() {
        DeviceSnapshot snapshot = snapshot(DEVICE_ID, DeviceStatus.ACTIVE);
        givenPending(DeviceSnapshotJson.toJson(snapshot));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(true);
        when(publishedDeviceStore.lastSnapshotAt(DEVICE_ID)).thenReturn(OptionalLong.of(500L));
        when(publishedDeviceStore.fingerprint(DEVICE_ID)).thenReturn(Optional.of(DeviceFingerprint.of(snapshot)));

        BatchResult result = service.publishPendingBatch();

        verify(publisher, never()).publish(any());
        verify(publishedDeviceStore).markSnapshotAt(DEVICE_ID, 1_000L);
        verify(publishedDeviceStore, never()).markFingerprint(any(), any());
        verify(publishedDeviceStore, never()).markPublished(any());
        assertThat(captureInsert(inOrder(outboxRepository)).status()).isEqualTo(OutboxStatus.SENT);
        assertThat(captureAudit().outcome()).isEqualTo(PublishOutcome.SKIPPED);
        assertThat(result.skipped()).isEqualTo(1);
        assertThat(result.published()).isZero();
    }

    @Test
    @DisplayName("Skipped snapshot does not move the remembered event time back")
    void givenSameFingerprintAndNewerPublished_whenPublishBatch_thenSnapshotTimeKept() {
        DeviceSnapshot snapshot = snapshot(DEVICE_ID, DeviceStatus.ACTIVE);
        givenPending(DeviceSnapshotJson.toJson(snapshot));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(true);
        when(publishedDeviceStore.lastSnapshotAt(DEVICE_ID)).thenReturn(OptionalLong.of(1_000L));
        when(publishedDeviceStore.fingerprint(DEVICE_ID)).thenReturn(Optional.of(DeviceFingerprint.of(snapshot)));

        service.publishPendingBatch();

        verify(publishedDeviceStore).markSnapshotAt(DEVICE_ID, 1_000L);
    }

    @Test
    @DisplayName("Device data changed then UPDATED, and the new fingerprint is stored only after Kafka acknowledged")
    void givenChangedFingerprint_whenPublishBatch_thenPublishedAndFingerprintStoredAfterAck() {
        DeviceSnapshot snapshot = snapshot(DEVICE_ID, DeviceStatus.ACTIVE);
        givenPending(DeviceSnapshotJson.toJson(snapshot));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(true);
        when(publishedDeviceStore.lastSnapshotAt(DEVICE_ID)).thenReturn(OptionalLong.of(500L));
        when(publishedDeviceStore.fingerprint(DEVICE_ID)).thenReturn(Optional.of("fingerprint-of-the-previous-data"));

        service.publishPendingBatch();

        InOrder order = inOrder(publisher, publishedDeviceStore, outboxRepository);
        ArgumentCaptor<DeviceInfo> info = ArgumentCaptor.forClass(DeviceInfo.class);
        order.verify(publisher).publish(info.capture());
        order.verify(publishedDeviceStore).markPublished(DEVICE_ID);
        order.verify(publishedDeviceStore).markSnapshotAt(DEVICE_ID, 1_000L);
        order.verify(publishedDeviceStore).markFingerprint(DEVICE_ID, DeviceFingerprint.of(snapshot));
        assertThat(captureInsert(order).status()).isEqualTo(OutboxStatus.SENT);
        assertThat(info.getValue().getEventType()).isEqualTo(DeviceUpsertEventType.UPDATED);
    }

    @Test
    @DisplayName("Known device without a stored fingerprint (published before it existed) then published once")
    void givenNoStoredFingerprint_whenPublishBatch_thenPublished() {
        DeviceSnapshot snapshot = snapshot(DEVICE_ID, DeviceStatus.ACTIVE);
        givenPending(DeviceSnapshotJson.toJson(snapshot));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(true);
        when(publishedDeviceStore.lastSnapshotAt(DEVICE_ID)).thenReturn(OptionalLong.empty());
        when(publishedDeviceStore.fingerprint(DEVICE_ID)).thenReturn(Optional.empty());

        service.publishPendingBatch();

        assertThat(capturePublished().getEventType()).isEqualTo(DeviceUpsertEventType.UPDATED);
        verify(publishedDeviceStore).markFingerprint(DEVICE_ID, DeviceFingerprint.of(snapshot));
    }

    @Test
    @DisplayName("New device is published even if Redis still holds the same fingerprint: devices:published decides CREATED")
    void givenNewDeviceWithKnownFingerprint_whenPublishBatch_thenCreated() {
        givenPending(DeviceSnapshotJson.toJson(snapshot(DEVICE_ID, DeviceStatus.ACTIVE)));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(false);

        service.publishPendingBatch();

        assertThat(capturePublished().getEventType()).isEqualTo(DeviceUpsertEventType.CREATED);
        verify(publishedDeviceStore, never()).fingerprint(any());
    }

    @Test
    @DisplayName("Kafka send fails then fingerprint is not stored: the change stays publishable")
    void givenKafkaFailure_whenPublishBatch_thenFingerprintNotStored() {
        givenPending(DeviceSnapshotJson.toJson(snapshot(DEVICE_ID, DeviceStatus.ACTIVE)));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(true);
        when(publishedDeviceStore.lastSnapshotAt(DEVICE_ID)).thenReturn(OptionalLong.empty());
        when(publishedDeviceStore.fingerprint(DEVICE_ID)).thenReturn(Optional.empty());
        doThrow(new RuntimeException("kafka rejected")).when(publisher).publish(any());

        service.publishPendingBatch();

        verify(publishedDeviceStore, never()).markFingerprint(any(), any());
        verify(publishedDeviceStore, never()).markSnapshotAt(any(), anyLong());
        assertThat(captureInsert(inOrder(outboxRepository)).status()).isEqualTo(OutboxStatus.FAILED);
    }

    @Test
    @DisplayName("Event without snapshot of a known device does not read the fingerprint")
    void givenTelemetry_whenPublishBatch_thenFingerprintNotRead() {
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(true);

        service.publishPendingBatch();

        verify(publishedDeviceStore, never()).fingerprint(any());
        verify(publishedDeviceStore, never()).markFingerprint(any(), any());
    }

    @Test
    @DisplayName("Stored snapshot unreadable then FAILED with attempts + 1, not an ERROR every iteration")
    void givenBrokenSnapshot_whenPublishBatch_thenFailed() {
        givenPending("{\"deviceId\":");
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(false);

        BatchResult result = service.publishPendingBatch();

        verify(publisher, never()).publish(any());
        OutboxRecord failed = captureInsert(inOrder(outboxRepository));
        assertThat(failed.status()).isEqualTo(OutboxStatus.FAILED);
        assertThat(failed.attempts()).isEqualTo(1);
        assertThat(result.failed()).isEqualTo(1);
    }

    private void givenPending(String snapshotJson) {
        pending = OutboxRecord.initial(EVENT_ID, DEVICE_ID, 1_000L, NOW.minusSeconds(5), TraceRef.NONE, snapshotJson);
        when(outboxRepository.findPending(MAX_ATTEMPTS, BATCH_SIZE)).thenReturn(List.of(pending));
    }

    private DeviceInfo capturePublished() {
        ArgumentCaptor<DeviceInfo> captor = ArgumentCaptor.forClass(DeviceInfo.class);
        verify(publisher).publish(captor.capture());
        return captor.getValue();
    }

    private static DeviceInfo forDevice(String deviceId) {
        return argThat(info -> info != null && deviceId.equals(info.getDeviceId()));
    }

    private PublicationAuditRecord captureAudit() {
        ArgumentCaptor<PublicationAuditRecord> captor = ArgumentCaptor.forClass(PublicationAuditRecord.class);
        verify(publicationAuditRepository).insert(captor.capture());
        return captor.getValue();
    }

    private OutboxRecord captureInsert(InOrder order) {
        ArgumentCaptor<OutboxRecord> captor = ArgumentCaptor.forClass(OutboxRecord.class);
        order.verify(outboxRepository).insert(captor.capture());
        return captor.getValue();
    }
}