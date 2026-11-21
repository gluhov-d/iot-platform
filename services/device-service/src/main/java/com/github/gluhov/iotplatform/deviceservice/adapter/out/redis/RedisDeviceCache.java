package com.github.gluhov.iotplatform.deviceservice.adapter.out.redis;

import com.github.gluhov.iotplatform.deviceservice.domain.exception.CacheUnavailableException;
import com.github.gluhov.iotplatform.deviceservice.domain.model.Device;
import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceCache;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * Кэш устройства: hash device:cache:v1:{deviceId} (body, version) со сроком DEVICE_CACHE_TTL. Порог версии живёт
 * в отдельном ключе device:floor:v1:{deviceId} со своим, более долгим сроком - его пишет device-collector-service
 * (ADR-0003). Запись - скриптом put-if-fresh.lua: отказ, если версия ниже порога или ниже уже закэшированной;
 * принятая запись удаляет ключ порога (реплика догнала применённую версию).
 * Ошибки Redis (включая тайм-аут REDIS_TIMEOUT) переводятся в CacheUnavailableException.
 */
@Component
public class RedisDeviceCache implements DeviceCache {

    private static final RedisScript<Long> PUT_IF_FRESH =
            RedisScript.of(new ClassPathResource("redis/put-if-fresh.lua"), Long.class);
    private static final String BODY = "body";
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final StringRedisTemplate redis;
    private final Duration ttl;

    public RedisDeviceCache(StringRedisTemplate redis, @Value("${app.cache.device-ttl}") Duration ttl) {
        this.redis = redis;
        this.ttl = ttl;
    }

    @Override
    public Optional<Device> find(String deviceId) {
        try {
            Object body = redis.opsForHash().get(key(deviceId), BODY);
            return body == null ? Optional.empty() : Optional.of(fromJson(body.toString()));
        } catch (DataAccessException | JacksonException e) {
            throw new CacheUnavailableException("Чтение кэша устройства", e);
        }
    }

    @Override
    public boolean putIfFresh(Device device) {
        try {
            Long stored = redis.execute(PUT_IF_FRESH, List.of(key(device.deviceId()), floorKey(device.deviceId())),
                    toJson(device), String.valueOf(device.version()), String.valueOf(ttl.toMillis()));
            return Long.valueOf(1L).equals(stored);
        } catch (DataAccessException | JacksonException e) {
            throw new CacheUnavailableException("Запись кэша устройства", e);
        }
    }

    static String key(String deviceId) {
        return "device:cache:v1:" + deviceId;
    }

    /** Порог версии - отдельный ключ: он должен переживать истечение срока ключа кэша (ADR-0003). */
    static String floorKey(String deviceId) {
        return "device:floor:v1:" + deviceId;
    }

    static String toJson(Device device) {
        return JSON.writeValueAsString(device);
    }

    static Device fromJson(String body) {
        return JSON.readValue(body, Device.class);
    }
}
