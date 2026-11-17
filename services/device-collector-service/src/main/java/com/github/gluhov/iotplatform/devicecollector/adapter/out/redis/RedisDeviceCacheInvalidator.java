package com.github.gluhov.iotplatform.devicecollector.adapter.out.redis;

import com.github.gluhov.iotplatform.devicecollector.domain.port.out.DeviceCacheInvalidator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * Сброс кэша устройства: скрипт raise-floor.lua поднимает порог версии в ключе device:v1:floor:{deviceId} и удаляет
 * устаревший ответ из hash device:v1:{deviceId}. Порог живёт дольше кэша (DEVICE_CACHE_FLOOR_TTL), поэтому отставание
 * реплики дольше срока кэша не возвращает старые данные (ADR-0003). Версия формата - v1, её читает device-service.
 * Redis недоступен - ошибка журналируется, исключение не бросается: обработка сообщения уже зафиксирована,
 * данные в кэше устареют не позже его срока.
 */
@Component
@Slf4j
public class RedisDeviceCacheInvalidator implements DeviceCacheInvalidator {
    private static final RedisScript<Long> RAISE_FLOOR =
            RedisScript.of(new ClassPathResource("redis/raise-floor.lua"), Long.class);

    private final StringRedisTemplate redis;
    private final Duration floorTtl;

    public RedisDeviceCacheInvalidator(StringRedisTemplate redis,
                                       @Value("${app.cache.floor-ttl}") Duration floorTtl) {
        this.redis = redis;
        this.floorTtl = floorTtl;
    }

    @Override
    public boolean raiseFloor(String deviceId, long version) {
        try {
            redis.execute(RAISE_FLOOR, List.of(key(deviceId), floorKey(deviceId)),
                    String.valueOf(version), String.valueOf(floorTtl.toMillis()));
            return true;
        } catch (DataAccessException e) {
            log.warn("Device cache floor not raised for {} (version {}): {}", deviceId, version, e.getMessage());
            return false;
        }
    }

    static String key(String deviceId) {
        return "device:v1:" + deviceId;
    }

    /** Порог версии - отдельный ключ со своим сроком жизни: он переживает истечение ключа кэша (ADR-0003). */
    static String floorKey(String deviceId) {
        return "device:v1:floor:" + deviceId;
    }
}
