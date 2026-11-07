package com.github.gluhov.iotplatform.devicecollector.domain.port.out;

/** Шард устройства без обращения к самому шарду - для метрик и логов на пути ошибки. */
public interface ShardResolver {

    /** Имя шарда или ProcessingResult.UNKNOWN_SHARD, если определить не удалось. */
    String shardOf(String deviceId);
}