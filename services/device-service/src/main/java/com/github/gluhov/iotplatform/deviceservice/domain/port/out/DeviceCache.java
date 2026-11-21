package com.github.gluhov.iotplatform.deviceservice.domain.port.out;

import com.github.gluhov.iotplatform.deviceservice.domain.exception.CacheUnavailableException;
import com.github.gluhov.iotplatform.deviceservice.domain.model.Device;

import java.util.Optional;

/** Кэш устройств (Cache-Aside). Недоступный кэш - исключение, решение о чтении из базы принимает сервис. */
public interface DeviceCache {

    /** @throws CacheUnavailableException кэш недоступен или значение не читается */
    Optional<Device> find(String deviceId);

    /**
     * Записывает карточку, если её версия не ниже наименьшей допустимой (floor) и не ниже уже закэшированной.
     *
     * @return true - записано; false - версия реплики устарела, запись отклонена
     * @throws CacheUnavailableException кэш недоступен
     */
    boolean putIfFresh(Device device);
}
