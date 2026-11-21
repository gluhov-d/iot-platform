package com.github.gluhov.iotplatform.devicecollector.domain.model;

import com.github.gluhov.iotplatform.devicecollector.domain.exception.DeviceNotStoredException;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.InvalidDeviceInfoException;

import java.util.Locale;

/** Почему сообщение не обработано - метка reason в devices_messages_failed_total и devices_processing_errors_total. */
public enum FailureReason {
    /** Сообщение невалидно: сразу в failed-events. */
    INVALID,
    /** Устройство не записано в PostgreSQL: повторы, затем failed-events. */
    STORAGE,
    /** Сообщение не десериализуется (не Avro, схемы нет в Schema Registry): сразу в failed-events. */
    DESERIALIZATION,
    /** Непредвиденная ошибка обработки: повторы, затем failed-events. */
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