package com.github.gluhov.iotplatform.devicecollector.domain.model;

/**
 * Итог обработки сообщения.
 *
 * @param shard   шард устройства (current_database() шарда, который выполнил запрос)
 * @param version версия записи после обработки: новая для APPLIED, текущая для остальных исходов (0 - устройства нет)
 */
public record ProcessingResult(ProcessingOutcome outcome, String shard, long version) {

    public static final String UNKNOWN_SHARD = "unknown";
}
