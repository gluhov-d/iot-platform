package com.github.gluhov.iotplatform.eventscollector.adapter.out.redis;

import com.github.gluhov.iotplatform.eventscollector.domain.port.out.PublishedDeviceStore;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class RedisPublishedDeviceStore implements PublishedDeviceStore {
    public static final String PUBLISHED_KEY = "devices:published";

    private final StringRedisTemplate redisTemplate;

    @Override
    public boolean isPublished(String deviceId) {
        return Boolean.TRUE.equals(redisTemplate.opsForSet().isMember(PUBLISHED_KEY, deviceId));
    }

    @Override
    public void markPublished(String deviceId) {
        redisTemplate.opsForSet().add(PUBLISHED_KEY, deviceId);
    }
}
