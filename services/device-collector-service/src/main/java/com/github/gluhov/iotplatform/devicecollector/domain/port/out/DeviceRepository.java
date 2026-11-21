package com.github.gluhov.iotplatform.devicecollector.domain.port.out;

import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceRecord;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceView;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessedEvent;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StoredDevice;
import com.github.gluhov.iotplatform.devicecollector.domain.model.WrittenDevice;

import java.time.Instant;
import java.util.Optional;

/**
 * Реестр устройств. Все запросы - по device_id (ключ шардирования). Методы записи вызываются внутри
 * TransactionRunner и возвращают шард (current_database()) и версию записи после запроса.
 * Ошибки доступа - StorageAccessException.
 */
public interface DeviceRepository {

    /** SELECT ... FOR UPDATE: сериализует обработку событий одного устройства. */
    Optional<StoredDevice> lockById(String deviceId);

    /** INSERT в журнал ON CONFLICT DO NOTHING: false - событие уже обработано (повторная доставка). */
    boolean markProcessed(ProcessedEvent event);

    /** Новое устройство или надгробие команды DELETE: версия 1, seen_count = 1. */
    WrittenDevice insert(DeviceRecord record);

    /** Событие роутера со снимком для устройства без ручных изменений: модель целиком, версия + 1. */
    WrittenDevice updateWithState(DeviceRecord record);

    /** Событие роутера без снимка для устройства без ручных изменений: статус и время последнего события, версия + 1. */
    WrittenDevice updateWithoutState(DeviceRecord record);

    /**
     * Событие роутера для устройства с ручными изменениями: записываются только данные устройства (firmware_version,
     * os_name, os_version, sensors, registered_at, last_seen_at); поля администратора и удаление не меняются.
     * Без снимка - только служебные поля и счётчики.
     */
    WrittenDevice updateDeviceData(DeviceRecord record);

    /**
     * Команда администратора, применённая к устройству: поля администратора из снимка (если он есть), статус,
     * удаление и managed_at; данные устройства не трогаются; версия + 1.
     */
    WrittenDevice updateAdmin(DeviceRecord record);

    /**
     * Событие, которое не меняет состояние (STALE, IGNORED): только seen_count + 1 и first_seen_at - самое раннее
     * известное событие. Версия не меняется.
     */
    WrittenDevice countStale(String deviceId, Instant eventAt);

    Optional<DeviceView> findById(String deviceId);
}
