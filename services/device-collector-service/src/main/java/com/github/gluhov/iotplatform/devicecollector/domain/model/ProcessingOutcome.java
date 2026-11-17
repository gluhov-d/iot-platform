package com.github.gluhov.iotplatform.devicecollector.domain.model;

import java.util.Locale;

public enum ProcessingOutcome {
    /** Новое событие: состояние устройства обновлено, версия устройства выросла. */
    APPLIED,
    /** Новое событие, но старше последней применённой команды или состояния: учтено в seen_count, состояние не меняется. */
    STALE,
    /**
     * Новое событие, которое не меняет устройство: UPDATE для удалённого устройства или событие роутера
     * для устройства, удалённого администратором. Учтено в seen_count.
     */
    IGNORED,
    /** Повторная доставка уже обработанного события: ничего не меняется. */
    DUPLICATE;

    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }
}
