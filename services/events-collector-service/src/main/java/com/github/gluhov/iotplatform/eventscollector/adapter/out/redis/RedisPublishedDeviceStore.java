package com.github.gluhov.iotplatform.eventscollector.adapter.out.redis;

import com.github.gluhov.iotplatform.eventscollector.domain.port.out.PublishedDeviceStore;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.OptionalLong;

@Component
@RequiredArgsConstructor
public class RedisPublishedDeviceStore implements PublishedDeviceStore {
    public static final String PUBLISHED_KEY = "devices:published";
    /** Хеш deviceId -> время события последнего опубликованного снимка (мс Unix). Пишет только владелец аренды. */
    public static final String SNAPSHOT_AT_KEY = "devices:snapshot-at";

    private final StringRedisTemplate redisTemplate;

    @Override
    public boolean isPublished(String deviceId) {
        return Boolean.TRUE.equals(redisTemplate.opsForSet().isMember(PUBLISHED_KEY, deviceId));
    }

    @Override
    public void markPublished(String deviceId) {
        redisTemplate.opsForSet().add(PUBLISHED_KEY, deviceId);
    }

    @Override
    public OptionalLong lastSnapshotAt(String deviceId) {
        Object value = redisTemplate.opsForHash().get(SNAPSHOT_AT_KEY, deviceId);
        return value == null ? OptionalLong.empty() : OptionalLong.of(Long.parseLong(value.toString()));
    }

    @Override
    public void markSnapshotAt(String deviceId, long eventTimeMs) {
        redisTemplate.opsForHash().put(SNAPSHOT_AT_KEY, deviceId, Long.toString(eventTimeMs));
    }
}