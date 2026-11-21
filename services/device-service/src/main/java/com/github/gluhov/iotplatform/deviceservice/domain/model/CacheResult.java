package com.github.gluhov.iotplatform.deviceservice.domain.model;

/** Исход поиска устройства в кэше: значение метки result счётчика device_cache_requests_total. */
public enum CacheResult {
    /** Карточка в кэше */
    HIT("hit"),
    /** Промах: карточка прочитана из модели чтения и записана в кэш; либо устройства нет */
    MISS("miss"),
    /** Реплика отдала версию ниже floor: ответ дан, в кэш не записан */
    STALE_REPLICA("stale_replica"),
    /** Кэш недоступен: ответ дан из модели чтения */
    ERROR("error");

    private final String label;

    CacheResult(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }
}
