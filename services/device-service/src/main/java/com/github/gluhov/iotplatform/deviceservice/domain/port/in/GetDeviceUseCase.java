package com.github.gluhov.iotplatform.deviceservice.domain.port.in;

import com.github.gluhov.iotplatform.deviceservice.domain.exception.DeviceNotFoundException;
import com.github.gluhov.iotplatform.deviceservice.domain.model.Device;

public interface GetDeviceUseCase {

    /**
     * Текущие данные устройства: из кэша, при промахе - из модели чтения.
     *
     * @throws DeviceNotFoundException устройства нет, оно удалено или у него нет карточки
     */
    Device getById(String deviceId);
}
