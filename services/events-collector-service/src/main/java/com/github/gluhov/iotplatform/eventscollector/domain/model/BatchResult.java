package com.github.gluhov.iotplatform.eventscollector.domain.model;

/**
 * @param deferred записи, отложенные до следующей итерации: публикатор недоступен или у устройства в этой пачке
 *                 уже была ошибка (порядок публикаций одного устройства сохраняется)
 */
public record BatchResult(boolean leaseAcquired, int selected, int published, int skipped, int failed, int errors,
                          boolean leaseLost, int deferred) {

    public static BatchResult leaseNotAcquired() {
        return new BatchResult(false, 0, 0, 0, 0, 0, false, 0);
    }
}