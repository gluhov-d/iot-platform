package com.github.gluhov.iotplatform.deviceservice.adapter.out.kafka;

import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceCommand;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceCommand.Operation;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceStatus;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import com.github.gluhov.iotplatform.events.avro.OperationType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Таблица «Сообщение device-service» (data-model.md): поля DeviceInfo по операциям. */
class DeviceInfoFactoryTest {

    private static final Instant ACCEPTED_AT = Instant.parse("2026-11-14T10:00:00.123Z");
    private static final UUID COMMAND_ID = UUID.fromString("5d1c2f3a-0000-4000-8000-000000000001");

    private final DeviceInfoFactory factory = new DeviceInfoFactory();

    @Test
    void givenUpdate_whenBuilt_thenUpdateMessageWithAdminFields() {
        DeviceCommand command = command(Operation.UPDATE, fields());

        DeviceInfo message = factory.toMessage(command);

        assertThat(message.getDeviceId()).isEqualTo("dev-1");
        assertThat(message.getOperation()).isEqualTo(OperationType.UPDATE);
        assertThat(message.getEventType()).isEqualTo(DeviceUpsertEventType.UPDATED);
        assertThat(message.getEventId()).isEqualTo(COMMAND_ID.toString());
        assertThat(message.getEmittedAtMs()).isEqualTo(ACCEPTED_AT.toEpochMilli());
        assertThat(message.getEventTimeMs()).isEqualTo(ACCEPTED_AT.toEpochMilli());
        assertThat(message.getDevice().getDeviceId()).isEqualTo("dev-1");
        assertThat(message.getDevice().getTenantId()).isEqualTo("t1");
        assertThat(message.getDevice().getDeviceType()).isEqualTo("sensor");
        assertThat(message.getDevice().getStatus()).isEqualTo(DeviceStatus.ACTIVE);
        assertThat(message.getDevice().getSettings()).containsEntry("mode", "eco");
        assertThat(message.getDevice().getAttributes()).containsEntry("zone", "A");
        assertThat(message.getDevice().getSensors()).isEmpty();
        assertThat(message.getDevice().getFirmwareVersion()).isNull();
    }

    @Test
    void givenCreate_whenBuilt_thenCreatedMessage() {
        DeviceInfo message = factory.toMessage(command(Operation.CREATE, fields()));

        assertThat(message.getOperation()).isEqualTo(OperationType.CREATE);
        assertThat(message.getEventType()).isEqualTo(DeviceUpsertEventType.CREATED);
        assertThat(message.getDevice()).isNotNull();
    }

    @Test
    void givenDelete_whenBuilt_thenDeletedMessageWithoutSnapshot() {
        DeviceInfo message = factory.toMessage(command(Operation.DELETE, null));

        assertThat(message.getOperation()).isEqualTo(OperationType.DELETE);
        assertThat(message.getEventType()).isEqualTo(DeviceUpsertEventType.DELETED);
        assertThat(message.getDevice()).isNull();
        assertThat(message.getDeviceId()).isEqualTo("dev-1");
    }

    private static DeviceCommand command(Operation operation, DeviceCommand.Fields fields) {
        return new DeviceCommand(COMMAND_ID, operation, "dev-1", fields, ACCEPTED_AT);
    }

    private static DeviceCommand.Fields fields() {
        return new DeviceCommand.Fields("name", "t1", "owner", "sensor", "SN-1", "ACME", "T100", "ACTIVE",
                Map.of("mode", "eco"), Map.of("zone", "A"));
    }
}
