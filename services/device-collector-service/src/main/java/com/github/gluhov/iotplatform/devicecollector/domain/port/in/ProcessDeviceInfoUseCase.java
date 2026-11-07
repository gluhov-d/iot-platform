package com.github.gluhov.iotplatform.devicecollector.domain.port.in;

import com.github.gluhov.iotplatform.devicecollector.domain.model.MessagePosition;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessingResult;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;

public interface ProcessDeviceInfoUseCase {

    /**
     * Проверяет сообщение и применяет его к реестру в одной транзакции на шарде устройства.
     *
     * @throws com.github.gluhov.iotplatform.devicecollector.domain.exception.InvalidDeviceInfoException сообщение невалидно
     * @throws com.github.gluhov.iotplatform.devicecollector.domain.exception.DeviceNotStoredException транзакция не выполнена
     */
    ProcessingResult process(String key, DeviceInfo info, MessagePosition position);
}