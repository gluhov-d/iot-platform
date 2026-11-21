package com.github.gluhov.iotplatform.devicecollector.adapter.out.redis;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RedisDeviceCacheInvalidatorTest {
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final RedisDeviceCacheInvalidator invalidator =
            new RedisDeviceCacheInvalidator(redis, Duration.ofHours(24));

    @Test
    @DisplayName("Applied version: raise-floor runs on the cache key and the separate floor key with the floor TTL")
    void givenAppliedVersion_whenRaiseFloor_thenScriptCalledWithBothKeysVersionAndFloorTtl() {
        boolean success = invalidator.raiseFloor("dev-1", 7L);

        assertThat(success).isTrue();
        // Порог - отдельный ключ со своим сроком (ADR-0003): срок кэша (минуты) ему не передаётся
        verify(redis).execute(any(RedisScript.class),
                eq(java.util.List.of("device:cache:v1:dev-1", "device:floor:v1:dev-1")), eq("7"), eq("86400000"));
    }

    @Test
    @DisplayName("Floor key is device:floor:v1:{deviceId} - it outlives the cache key")
    void givenDeviceId_whenFloorKey_thenSeparateKeyWithFloorPrefix() {
        assertThat(RedisDeviceCacheInvalidator.floorKey("dev-1")).isEqualTo("device:floor:v1:dev-1");
        assertThat(RedisDeviceCacheInvalidator.floorKey("dev-1"))
                .isNotEqualTo(RedisDeviceCacheInvalidator.key("dev-1"));
    }

    @Test
    @DisplayName("Device id with the floor prefix: the cache key of \"floor:dev-42\" is not the floor key of \"dev-42\"")
    void givenDeviceIdWithFloorPrefix_whenKeys_thenCacheAndFloorKeysNeverCollide() {
        assertThat(RedisDeviceCacheInvalidator.key("floor:dev-42"))
                .isNotEqualTo(RedisDeviceCacheInvalidator.floorKey("dev-42"));
        String cachePrefix = RedisDeviceCacheInvalidator.key("");
        String floorPrefix = RedisDeviceCacheInvalidator.floorKey("");
        assertThat(cachePrefix).doesNotStartWith(floorPrefix);
        assertThat(floorPrefix).doesNotStartWith(cachePrefix);
    }

    @Test
    @DisplayName("Redis unavailable: no exception, false returned so the metric counts an error")
    void givenRedisDown_whenRaiseFloor_thenNoExceptionAndFalse() {
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString()))
                .thenThrow(new RedisConnectionFailureException("connection refused"));

        assertThatCode(() -> assertThat(invalidator.raiseFloor("dev-1", 7L)).isFalse())
                .doesNotThrowAnyException();
    }
}
