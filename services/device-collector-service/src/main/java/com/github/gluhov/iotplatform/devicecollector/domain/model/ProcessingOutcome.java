package com.github.gluhov.iotplatform.devicecollector.domain.model;

import java.util.Locale;

public enum ProcessingOutcome {
    /** Новое событие: состояние устройства обновлено. */
    APPLIED,
    /** Новое событие, но старше уже сохранённого: учтено в seen_count, состояние не меняется. */
    STALE,
    /** Повторная доставка уже обработанного события: ничего не меняется. */
    DUPLICATE;

    public String label() {
        return name().toLowerCase(Locale.ROOT);
    }
}