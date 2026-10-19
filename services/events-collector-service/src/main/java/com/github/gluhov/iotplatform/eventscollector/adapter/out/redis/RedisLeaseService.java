package com.github.gluhov.iotplatform.eventscollector.adapter.out.redis;

import com.github.gluhov.iotplatform.eventscollector.domain.port.out.PublisherLease;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.List;

@Component
@RequiredArgsConstructor
public class RedisLeaseService implements PublisherLease {
    public static final String LEASE_KEY = "outbox:lease";

    // Проверка владельца и удаление атомарны: истекшую аренду, взятую другим экземпляром, не удалить
    public static final RedisScript<Long> RELEASE_SCRIPT = new DefaultRedisScript<>("""
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("del", KEYS[1])
                else
                    return 0
                end
            """, Long.class);

    // Продлевается только своя аренда: истекшую и взятую другим экземпляром продлить нельзя
    public static final RedisScript<Long> EXTEND_SCRIPT = new DefaultRedisScript<>("""
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("pexpire", KEYS[1], ARGV[2])
                else
                    return 0
                end
            """, Long.class);

    private final StringRedisTemplate redisTemplate;

    @Override
    public Optional<String> tryAcquire(Duration ttl) {
        String ownerToken = UUID.randomUUID().toString();
        Boolean acquired = redisTemplate.execute((RedisCallback<Boolean>) connection -> connection.stringCommands().set(
                        LEASE_KEY.getBytes(StandardCharsets.UTF_8),
                        ownerToken.getBytes(StandardCharsets.UTF_8),
                        Expiration.milliseconds(ttl.toMillis()),
                        SetOption.ifAbsent()));
        return Boolean.TRUE.equals(acquired) ? Optional.of(ownerToken) : Optional.empty();
    }

    @Override
    public boolean extend(String ownerToken, Duration ttl) {
        Long extended = redisTemplate.execute(EXTEND_SCRIPT, List.of(LEASE_KEY), ownerToken, String.valueOf(ttl.toMillis()));
        return extended != null && extended == 1L;
    }

    @Override
    public boolean release(String ownerToken) {
        Long deleted = redisTemplate.execute(RELEASE_SCRIPT, List.of(LEASE_KEY), ownerToken);
        return deleted != null && deleted == 1L;
    }
}
