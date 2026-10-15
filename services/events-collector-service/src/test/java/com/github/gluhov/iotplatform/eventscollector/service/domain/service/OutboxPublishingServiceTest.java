package com.github.gluhov.iotplatform.eventscollector.service.domain.service;

import com.github.gluhov.iotplatform.eventscollector.domain.model.*;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.*;
import com.github.gluhov.iotplatform.eventscollector.domain.service.OutboxPublishingService;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
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
        order.verify(publisher).publish(DEVICE_ID, NOW.toEpochMilli());
        order.verify(publishedDeviceStore).markPublished(DEVICE_ID);
        OutboxRecord sent = captureInsert(order);
        order.verify(lease).release(TOKEN);

        assertThat(sent.status()).isEqualTo(OutboxStatus.SENT);
        assertThat(sent.stateVersion()).isEqualTo(pending.stateVersion() + 1);
        assertThat(sent.updatedAt()).isEqualTo(NOW);
        assertThat(sent.createdAt()).isEqualTo(pending.createdAt());
        assertThat(result.published()).isEqualTo(1);
        verify(metrics).publishSucceeded();
    }

    @Test
    @DisplayName("Publish already published device then skip with SENT version")
    void givenPublishedDevice_whenPublishBatch_thenSkipWithSentVersion() {
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(true);

        BatchResult result = service.publishPendingBatch();

        verify(publisher, never()).publish(anyString(), anyLong());
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
        doThrow(new IllegalStateException("broker unavailable")).when(publisher).publish(eq(DEVICE_ID), anyLong());

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
        verify(publisher, never()).publish(anyString(), anyLong());
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
        verify(publisher).publish(eq("dev-2"), anyLong());
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
    @DisplayName("Batch of several records then lease extended before each next record")
    void givenSeveralRecords_whenPublishBatch_thenLeaseExtendedBeforeEachNextRecord() {
        OutboxRecord second = OutboxRecord.initial("evt-2", "dev-2", 2_000L, NOW, TraceRef.NONE);
        OutboxRecord third = OutboxRecord.initial("evt-3", "dev-3", 3_000L, NOW, TraceRef.NONE);
        when(outboxRepository.findPending(MAX_ATTEMPTS, BATCH_SIZE)).thenReturn(List.of(pending, second, third));
        when(publishedDeviceStore.isPublished(anyString())).thenReturn(false);

        BatchResult result = service.publishPendingBatch();

        InOrder order = inOrder(publisher, lease);
        order.verify(publisher).publish(eq(DEVICE_ID), anyLong());
        order.verify(lease).extend(TOKEN, TTL);
        order.verify(publisher).publish(eq("dev-2"), anyLong());
        order.verify(lease).extend(TOKEN, TTL);
        order.verify(publisher).publish(eq("dev-3"), anyLong());
        order.verify(lease).release(TOKEN);
        verify(lease, times(2)).extend(TOKEN, TTL);
        assertThat(result.published()).isEqualTo(3);
        assertThat(result.leaseLost()).isFalse();
    }

    @Test
    @DisplayName("Lease lost during batch then remaining records not published")
    void givenLeaseLostMidBatch_whenPublishBatch_thenBatchStops() {
        OutboxRecord second = OutboxRecord.initial("evt-2", "dev-2", 2_000L, NOW, TraceRef.NONE);
        when(outboxRepository.findPending(MAX_ATTEMPTS, BATCH_SIZE)).thenReturn(List.of(pending, second));
        when(publishedDeviceStore.isPublished(DEVICE_ID)).thenReturn(false);
        when(lease.extend(TOKEN, TTL)).thenReturn(false);
        when(lease.release(TOKEN)).thenReturn(false);

        BatchResult result = service.publishPendingBatch();

        verify(publisher).publish(eq(DEVICE_ID), anyLong());
        verify(publisher, never()).publish(eq("dev-2"), anyLong());
        verify(outboxRepository, never()).insert(argThat(r -> "evt-2".equals(r.eventId())));
        verify(lease).release(TOKEN);
        assertThat(result.leaseLost()).isTrue();
        assertThat(result.selected()).isEqualTo(2);
        assertThat(result.published()).isEqualTo(1);
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
        }).when(publisher).publish(eq(DEVICE_ID), anyLong());

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
        doThrow(new IllegalStateException("broker unavailable")).when(publisher).publish(eq(DEVICE_ID), anyLong());

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
