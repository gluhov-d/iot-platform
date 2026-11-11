package com.github.gluhov.iotplatform.devicecollector.service.adapter.in.kafka;

import ch.qos.logback.classic.Level;
import com.github.gluhov.iotplatform.devicecollector.adapter.in.kafka.DeviceInfoListener;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.DeviceNotStoredException;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.InvalidDeviceInfoException;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.StorageAccessException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.MessagePosition;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessingOutcome;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessingResult;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StorageFailureReason;
import com.github.gluhov.iotplatform.devicecollector.domain.port.in.ProcessDeviceInfoUseCase;
import com.github.gluhov.iotplatform.devicecollector.support.LogCapture;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.ByteBuffer;

import static com.github.gluhov.iotplatform.devicecollector.data.TestData.T0;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.info;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DeviceInfoListenerTest {
    private static final MessagePosition POSITION = new MessagePosition("devices", 1, 42L);

    private final ProcessDeviceInfoUseCase useCase = mock(ProcessDeviceInfoUseCase.class);
    private final Acknowledgment ack = mock(Acknowledgment.class);
    private final DeviceInfoListener listener = new DeviceInfoListener(useCase);
    private final DeviceInfo message = info("dev-1", "evt-1", DeviceUpsertEventType.CREATED, T0).build();
    private final ConsumerRecord<String, DeviceInfo> record = new ConsumerRecord<>("devices", 1, 42L, "dev-1", message);

    @Test
    @DisplayName("Stored: offset acknowledged after processing, JSON log has shard, status and message position")
    void givenStored_whenConsume_thenAcknowledgedAndLogged() {
        when(useCase.process(eq("dev-1"), eq(message), eq(POSITION)))
                .thenReturn(new ProcessingResult(ProcessingOutcome.APPLIED, "shard_1"));

        try (LogCapture logs = LogCapture.of(DeviceInfoListener.class)) {
            listener.onDeviceInfo(record, ack);

            assertThat(logs.single(Level.INFO, "Device info applied, offset acknowledged").getMDCPropertyMap())
                    .containsEntry("shard", "shard_1")
                    .containsEntry("status", "APPLIED")
                    .containsEntry("deviceId", "dev-1")
                    .containsEntry("eventId", "evt-1")
                    .containsEntry("eventType", "CREATED")
                    .containsEntry("topic", "devices")
                    .containsEntry("partition", "1")
                    .containsEntry("offset", "42");
        }
        verify(ack).acknowledge();
        assertThat(MDC.get("deviceId")).as("MDC cleaned up").isNull();
    }

    @Test
    @DisplayName("Redelivery after an error: delivery attempt from the kafka_deliveryAttempt header is passed on")
    void givenDeliveryAttemptHeader_whenConsume_thenAttemptInPosition() {
        record.headers().add(KafkaHeaders.DELIVERY_ATTEMPT, ByteBuffer.allocate(Integer.BYTES).putInt(3).array());
        when(useCase.process(eq("dev-1"), eq(message), eq(new MessagePosition("devices", 1, 42L, 3))))
                .thenReturn(new ProcessingResult(ProcessingOutcome.APPLIED, "shard_1"));

        listener.onDeviceInfo(record, ack);

        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("Not stored: offset not acknowledged, error rethrown for the error handler, shard from the error")
    void givenStorageFailure_whenConsume_thenNotAcknowledgedAndRethrown() {
        DeviceNotStoredException failure = new DeviceNotStoredException("dev-1", "shard_0",
                new StorageAccessException(StorageFailureReason.CONNECTION, "shard is down", null));
        when(useCase.process(any(), any(), any())).thenThrow(failure);

        try (LogCapture logs = LogCapture.of(DeviceInfoListener.class)) {
            assertThatThrownBy(() -> listener.onDeviceInfo(record, ack)).isSameAs(failure);

            assertThat(logs.single(Level.WARN, "Device info not processed, offset not acknowledged").getMDCPropertyMap())
                    .containsEntry("status", "FAILED")
                    .containsEntry("shard", "shard_0")
                    .containsEntry("errorClass", DeviceNotStoredException.class.getName())
                    .containsEntry("deviceId", "dev-1");
        }
        verify(ack, never()).acknowledge();
    }

    @Test
    @DisplayName("Invalid: rejected, shard unknown, offset not acknowledged here (devices-dlt does it)")
    void givenInvalid_whenConsume_thenRejected() {
        when(useCase.process(any(), any(), any())).thenThrow(new InvalidDeviceInfoException("deviceId is blank"));

        try (LogCapture logs = LogCapture.of(DeviceInfoListener.class)) {
            assertThatThrownBy(() -> listener.onDeviceInfo(record, ack)).isInstanceOf(InvalidDeviceInfoException.class);

            assertThat(logs.single(Level.WARN, "Device info not processed, offset not acknowledged").getMDCPropertyMap())
                    .containsEntry("status", "REJECTED")
                    .containsEntry("shard", "unknown")
                    .containsEntry("errorMessage", "deviceId is blank");
        }
        verify(ack, never()).acknowledge();
    }

    @Test
    @DisplayName("Tombstone (null value): deviceId for logs taken from the key")
    void givenTombstone_whenConsume_thenKeyInLogs() {
        when(useCase.process(any(), any(), any())).thenThrow(new InvalidDeviceInfoException("Message value is empty"));

        try (LogCapture logs = LogCapture.of(DeviceInfoListener.class)) {
            assertThatThrownBy(() -> listener.onDeviceInfo(new ConsumerRecord<>("devices", 1, 43L, "dev-1", null), ack))
                    .isInstanceOf(InvalidDeviceInfoException.class);

            assertThat(logs.single(Level.WARN, "Device info not processed, offset not acknowledged").getMDCPropertyMap())
                    .containsEntry("deviceId", "dev-1");
        }
    }
}