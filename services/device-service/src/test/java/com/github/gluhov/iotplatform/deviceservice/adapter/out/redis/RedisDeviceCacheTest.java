package com.github.gluhov.iotplatform.deviceservice.adapter.out.redis;

import com.github.gluhov.iotplatform.deviceservice.domain.exception.CacheUnavailableException;
import com.github.gluhov.iotplatform.deviceservice.domain.model.Device;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceSensor;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.RedisConnectionFailureException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class RedisDeviceCacheTest {

    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final HashOperations<String, Object, Object> hash = mock(HashOperations.class);
    private final RedisDeviceCache cache = new RedisDeviceCache(redis, Duration.ofMinutes(10));

    @Test
    void givenNoBody_whenFind_thenEmptyAndKeyWithVersionPrefix() {
        when(redis.opsForHash()).thenReturn(hash);
        when(hash.get("device:cache:v1:dev-1", "body")).thenReturn(null);

        assertThat(cache.find("dev-1")).isEmpty();
    }

    @Test
    void givenDeviceIdWithFloorPrefix_whenKeys_thenCacheAndFloorKeysNeverCollide() {
        assertThat(RedisDeviceCache.key("floor:dev-42")).isNotEqualTo(RedisDeviceCache.floorKey("dev-42"));
        // Ни один префикс не начинает другой: допустимый deviceId (FR-004, «:») не превратит ключ кэша в ключ порога
        String cachePrefix = RedisDeviceCache.key("");
        String floorPrefix = RedisDeviceCache.floorKey("");
        assertThat(cachePrefix).doesNotStartWith(floorPrefix);
        assertThat(floorPrefix).doesNotStartWith(cachePrefix);
    }

    @Test
    void givenStoredBody_whenFind_thenDeviceParsed() {
        Device device = device("dev-1", 7);
        when(redis.opsForHash()).thenReturn(hash);
        when(hash.get("device:cache:v1:dev-1", "body")).thenReturn(RedisDeviceCache.toJson(device));

        Optional<Device> found = cache.find("dev-1");

        assertThat(found).contains(device);
    }

    @Test
    void givenJsonRoundTrip_whenSerialized_thenSameDevice() {
        Device device = device("dev-2", 1);

        assertThat(RedisDeviceCache.fromJson(RedisDeviceCache.toJson(device))).isEqualTo(device);
    }

    @Test
    void givenScriptStores_whenPutIfFresh_thenTrue() {
        when(redis.execute(ArgumentMatchers.<org.springframework.data.redis.core.script.RedisScript<Long>>any(),
                anyList(), ArgumentMatchers.<Object>any(), ArgumentMatchers.<Object>any(),
                ArgumentMatchers.<Object>any())).thenReturn(1L);

        assertThat(cache.putIfFresh(device("dev-1", 7))).isTrue();
    }

    @Test
    void givenScriptRejects_whenPutIfFresh_thenFalse() {
        when(redis.execute(ArgumentMatchers.<org.springframework.data.redis.core.script.RedisScript<Long>>any(),
                anyList(), ArgumentMatchers.<Object>any(), ArgumentMatchers.<Object>any(),
                ArgumentMatchers.<Object>any())).thenReturn(0L);

        assertThat(cache.putIfFresh(device("dev-1", 7))).isFalse();
    }

    @Test
    void givenPutIfFresh_whenCalled_thenCacheAndFloorKeysPassedWithCacheTtl() {
        when(redis.execute(ArgumentMatchers.<org.springframework.data.redis.core.script.RedisScript<Long>>any(),
                anyList(), ArgumentMatchers.<Object>any(), ArgumentMatchers.<Object>any(),
                ArgumentMatchers.<Object>any())).thenReturn(1L);

        cache.putIfFresh(device("dev-1", 7));

        // Порог - отдельный ключ (ADR-0003): скрипт получает оба и TTL кэша
        verify(redis).execute(ArgumentMatchers.<org.springframework.data.redis.core.script.RedisScript<Long>>any(),
                eq(List.of("device:cache:v1:dev-1", "device:floor:v1:dev-1")),
                any(), eq("7"), eq("600000"));
        assertThat(RedisDeviceCache.floorKey("dev-1")).isEqualTo("device:floor:v1:dev-1");
    }

    @Test
    void givenRedisDown_whenFind_thenCacheUnavailable() {
        when(redis.opsForHash()).thenReturn(hash);
        when(hash.get("device:cache:v1:dev-1", "body")).thenThrow(new RedisConnectionFailureException("down"));

        assertThatThrownBy(() -> cache.find("dev-1")).isInstanceOf(CacheUnavailableException.class);
    }

    @Test
    void givenRedisTimeout_whenPutIfFresh_thenCacheUnavailable() {
        when(redis.execute(ArgumentMatchers.<org.springframework.data.redis.core.script.RedisScript<Long>>any(),
                anyList(), ArgumentMatchers.<Object>any(), ArgumentMatchers.<Object>any(),
                ArgumentMatchers.<Object>any())).thenThrow(new QueryTimeoutException("timeout"));

        assertThatThrownBy(() -> cache.putIfFresh(device("dev-1", 7)))
                .isInstanceOf(CacheUnavailableException.class);
    }

    private static Device device(String id, long version) {
        return new Device(id, "name", "t1", "o1", "sensor", "SN-1", "acme", "m1", "ACTIVE",
                Map.of("k", "v"), Map.of(), "1.0", "linux", "6",
                List.of(new DeviceSensor("s1", "temp", "C", "21", Instant.parse("2026-01-01T00:00:00Z"), "OK")),
                Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-01-02T00:00:00Z"), version);
    }
}
