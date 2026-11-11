package com.github.gluhov.iotplatform.devicecollector.domain.port.out;

import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceRecord;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceView;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessedEvent;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StoredDevice;

import java.time.Instant;
import java.util.Optional;

/**
 * Реестр устройств. Все запросы - по device_id (ключ шардирования). Методы записи вызываются внутри
 * TransactionRunner и возвращают шард (current_database()), который выполнил запрос.
 * Ошибки доступа - StorageAccessException.
 */
public interface DeviceRepository {

    /** SELECT ... FOR UPDATE: сериализует обработку событий одного устройства. */
    Optional<StoredDevice> lockById(String deviceId);

    /** INSERT в журнал ON CONFLICT DO NOTHING: false - событие уже обработано (повторная доставка). */
    boolean markProcessed(ProcessedEvent event);

    /** Новое устройство: seen_count = 1. */
    String insert(DeviceRecord record);

    /** Событие со снимком: модель устройства целиком, seen_count + 1. */
    String updateWithState(DeviceRecord record);

    /** Событие без снимка: статус и время последнего события, seen_count + 1. */
    String updateWithoutState(DeviceRecord record);

    /** Устаревшее событие: состояние не меняется, seen_count + 1; first_seen_at - самое раннее известное событие. */
    String countStale(String deviceId, Instant eventAt);

    Optional<DeviceView> findById(String deviceId);
}