package com.github.gluhov.iotplatform.deviceservice.domain.port.in;

import com.github.gluhov.iotplatform.deviceservice.domain.exception.CommandNotAcceptedException;
import com.github.gluhov.iotplatform.deviceservice.domain.exception.DeviceAlreadyExistsException;
import com.github.gluhov.iotplatform.deviceservice.domain.exception.DeviceNotFoundException;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceCommand;

/** Команды администратора: запись асинхронная - результат команда получает от Kafka, применяет device-collector-service. */
public interface ManageDeviceUseCase {

    /**
     * Команда CREATE.
     *
     * @throws DeviceAlreadyExistsException устройство уже есть (не удалено)
     * @throws CommandNotAcceptedException  Kafka не подтвердила команду за срок ожидания
     */
    DeviceCommand create(String deviceId, DeviceCommand.Fields fields);

    /**
     * Команда UPDATE: заменяет поля администратора.
     *
     * @throws DeviceNotFoundException     устройства нет или оно удалено
     * @throws CommandNotAcceptedException  Kafka не подтвердила команду за срок ожидания
     */
    DeviceCommand update(String deviceId, DeviceCommand.Fields fields);

    /**
     * Команда DELETE (мягкое удаление).
     *
     * @throws DeviceNotFoundException     устройства нет или оно удалено
     * @throws CommandNotAcceptedException  Kafka не подтвердила команду за срок ожидания
     */
    DeviceCommand delete(String deviceId);
}
