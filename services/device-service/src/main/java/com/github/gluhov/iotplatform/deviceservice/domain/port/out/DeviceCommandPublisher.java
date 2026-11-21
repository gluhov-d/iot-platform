package com.github.gluhov.iotplatform.deviceservice.domain.port.out;

import com.github.gluhov.iotplatform.deviceservice.domain.exception.CommandNotAcceptedException;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceCommand;

/** Отправка команды в тему devices. Возврат без исключения - брокер подтвердил запись. */
public interface DeviceCommandPublisher {

    /** @throws CommandNotAcceptedException подтверждения нет (брокер недоступен, истёк срок ожидания, схема не найдена) */
    void publish(DeviceCommand command);
}
