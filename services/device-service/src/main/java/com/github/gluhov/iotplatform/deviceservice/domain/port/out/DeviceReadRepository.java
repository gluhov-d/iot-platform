package com.github.gluhov.iotplatform.deviceservice.domain.port.out;

import com.github.gluhov.iotplatform.deviceservice.domain.exception.ReadModelUnavailableException;
import com.github.gluhov.iotplatform.deviceservice.domain.model.Device;

import java.util.Optional;

/** Модель чтения (реплики шардов). */
public interface DeviceReadRepository {

    /**
     * Карточка устройства по deviceId. Пусто, если строки нет, устройство удалено или карточки нет.
     *
     * @throws ReadModelUnavailableException модель чтения недоступна или не ответила за тайм-аут
     */
    Optional<Device> findById(String deviceId);
}
