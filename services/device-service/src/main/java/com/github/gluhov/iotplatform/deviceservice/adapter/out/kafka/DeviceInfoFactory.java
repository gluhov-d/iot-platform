package com.github.gluhov.iotplatform.deviceservice.adapter.out.kafka;

import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceCommand;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceSnapshot;
import com.github.gluhov.iotplatform.events.avro.DeviceStatus;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import com.github.gluhov.iotplatform.events.avro.OperationType;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Команда → DeviceInfo (таблица «Сообщение device-service», data-model.md): eventId = commandId, время приёма - в
 * eventTimeMs и emittedAtMs. У DELETE снимка нет (device = null); у CREATE и UPDATE снимок несёт поля администратора,
 * остальные поля устройства пусты - их сообщает само устройство.
 */
@Component
public class DeviceInfoFactory {

    public DeviceInfo toMessage(DeviceCommand command) {
        long acceptedAtMs = command.acceptedAt().toEpochMilli();
        return DeviceInfo.newBuilder()
                .setDeviceId(command.deviceId())
                .setEmittedAtMs(acceptedAtMs)
                .setEventTimeMs(acceptedAtMs)
                .setEventId(command.commandId().toString())
                .setEventType(eventTypeOf(command.operation()))
                .setOperation(OperationType.valueOf(command.operation().name()))
                .setDevice(snapshotOf(command))
                .build();
    }

    private static DeviceUpsertEventType eventTypeOf(DeviceCommand.Operation operation) {
        return switch (operation) {
            case CREATE -> DeviceUpsertEventType.CREATED;
            case UPDATE -> DeviceUpsertEventType.UPDATED;
            case DELETE -> DeviceUpsertEventType.DELETED;
        };
    }

    private static DeviceSnapshot snapshotOf(DeviceCommand command) {
        if (command.operation() == DeviceCommand.Operation.DELETE) {
            return null;
        }
        DeviceCommand.Fields fields = command.fields();
        return DeviceSnapshot.newBuilder()
                .setDeviceId(command.deviceId())
                .setTenantId(fields.tenantId())
                .setOwnerId(fields.ownerId())
                .setDeviceType(fields.deviceType())
                .setSerialNumber(fields.serialNumber())
                .setManufacturer(fields.manufacturer())
                .setModel(fields.model())
                .setName(fields.name())
                .setStatus(DeviceStatus.valueOf(fields.status()))
                .setSettings(fields.settings())
                .setAttributes(fields.attributes())
                .setSensors(List.of())
                .build();
    }
}
