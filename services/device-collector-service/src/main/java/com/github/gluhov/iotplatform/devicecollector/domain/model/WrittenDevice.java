package com.github.gluhov.iotplatform.devicecollector.domain.model;

/**
 * Результат записи строки devices: шард, который выполнил запрос (current_database()), и версия записи после него
 * (RETURNING current_database(), version).
 */
public record WrittenDevice(String shard, long version) {}
