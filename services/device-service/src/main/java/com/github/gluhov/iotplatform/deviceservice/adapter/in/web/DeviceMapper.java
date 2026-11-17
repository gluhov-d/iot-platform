package com.github.gluhov.iotplatform.deviceservice.adapter.in.web;

import com.github.gluhov.iotplatform.deviceservice.api.model.DeviceCommandAccepted;
import com.github.gluhov.iotplatform.deviceservice.api.model.DeviceCreateRequest;
import com.github.gluhov.iotplatform.deviceservice.api.model.DeviceResponse;
import com.github.gluhov.iotplatform.deviceservice.api.model.DeviceUpdateRequest;
import com.github.gluhov.iotplatform.deviceservice.api.model.Sensor;
import com.github.gluhov.iotplatform.deviceservice.domain.model.Device;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceCommand;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceSensor;
import org.mapstruct.Mapper;
import org.mapstruct.ReportingPolicy;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * Device → DeviceResponse, запрос → поля команды, DeviceCommand → DeviceCommandAccepted. Незаполненное поле ответа -
 * ошибка сборки (unmappedTargetPolicy = ERROR). Статус в модели уже из перечисления контракта (JDBC-адаптер приводит
 * прочее к UNKNOWN).
 */
@Mapper(componentModel = "spring", unmappedTargetPolicy = ReportingPolicy.ERROR)
public interface DeviceMapper {

    DeviceResponse toResponse(Device device);

    Sensor toSensor(DeviceSensor sensor);

    DeviceCommand.Fields toFields(DeviceUpdateRequest request);

    DeviceCommand.Fields toFields(DeviceCreateRequest request);

    DeviceCommandAccepted toAccepted(DeviceCommand command);

    default OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }
}
