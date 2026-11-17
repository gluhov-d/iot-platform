package com.github.gluhov.iotplatform.devicecollector.domain.port.out;

/**
 * Сброс кэша устройства после фиксации изменения: кэш device-service не должен отдавать данные старее версии
 * {@code version} (floor в Redis). Вызывается только после COMMIT и только для APPLIED.
 */
public interface DeviceCacheInvalidator {

    /**
     * Поднимает floor устройства до {@code version}.
     *
     * @return true - сброс выполнен; false - Redis недоступен. Исключение не бросается: результат обработки
     *         сообщения от сброса не зависит
     */
    boolean raiseFloor(String deviceId, long version);
}
