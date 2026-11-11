package com.github.gluhov.iotplatform.devicecollector.domain.model;

import com.github.gluhov.iotplatform.devicecollector.domain.exception.DeviceNotStoredException;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.InvalidDeviceInfoException;

import java.util.Locale;

/** Почему сообщение не обработано - метка reason в devices_messages_failed_total и devices_dlt_published_total. */
public enum FailureReason {
    /** Сообщение невалидно: сразу в devices-dlt. */
    INVALID,
    /** Устройство не записано в PostgreSQL: повторы, затем devices-dlt. */
    STORAGE,
    /** Сообщение не десериализуется (не Avro, схемы нет в Schema Registry): сразу в devices-dlt. */
    DESERIALIZATION,
    /** Непредвиденная ошибка обработки: повторы, затем devices-dlt. */
    ERROR;

    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** Причина по исключению обработчика (в том числе обёрнутому контейнером Kafka). */
    public static FailureReason of(Throwable exception) {
        for (Throwable e = exception; e != null; e = e.getCause() == e ? null : e.getCause()) {
            if (e instanceof InvalidDeviceInfoException) {
                return INVALID;
            }
            if (e instanceof DeviceNotStoredException) {
                return STORAGE;
            }
        }
        return ERROR;
    }
}