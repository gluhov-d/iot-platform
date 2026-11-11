package com.github.gluhov.iotplatform.eventscollector.adapter.out.redis;

import com.github.gluhov.iotplatform.eventscollector.domain.model.PublicationMarkResult;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.PublishedDeviceStore;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.OptionalLong;
import java.util.Optional;

import static com.github.gluhov.iotplatform.eventscollector.adapter.out.redis.RedisLeaseService.LEASE_KEY;

@Component
@RequiredArgsConstructor
public class RedisPublishedDeviceStore implements PublishedDeviceStore {
    public static final String PUBLISHED_KEY = "devices:published";
    /** Хеш deviceId -> время события последнего опубликованного снимка (мс Unix). Пишет только владелец аренды. */
    public static final String SNAPSHOT_AT_KEY = "devices:snapshot-at";
    /** Хеш deviceId -> SHA-256 данных устройства из последнего опубликованного снимка. Пишется после подтверждения Kafka. */
    public static final String FINGERPRINT_KEY = "devices:fingerprint";

    // Отметки публикации одной атомарной операцией: аренда, devices:published, время снимка и отпечаток не могут
    // разойтись, а процесс, у которого аренду уже забрали, ничего не запишет (ключи - в одном экземпляре Redis, не в кластере).
    // KEYS: 1 - аренда, 2 - devices:published, 3 - devices:snapshot-at, 4 - devices:fingerprint.
    // ARGV: 1 - ownerToken, 2 - deviceId, 3 - "1": отметить опубликованным, 4 - время снимка ("" - нет), 5 - отпечаток ("" - нет).
    // Результат: -1 - аренда не наша, ничего не записано; 0 - в Redis снимок новее: время и отпечаток не тронуты;
    // 1 - записано. Время снимка и отпечаток пишутся вместе: отпечаток всегда относится к снимку с запомненным временем
    public static final RedisScript<Long> MARK_PUBLICATION_SCRIPT = new DefaultRedisScript<>("""
                if redis.call("get", KEYS[1]) ~= ARGV[1] then
                    return -1
                end
                if ARGV[3] == "1" then
                    redis.call("sadd", KEYS[2], ARGV[2])
                end
                if ARGV[4] == "" then
                    return 1
                end
                local current = redis.call("hget", KEYS[3], ARGV[2])
                if current and tonumber(current) > tonumber(ARGV[4]) then
                    return 0
                end
                redis.call("hset", KEYS[3], ARGV[2], ARGV[4])
                if ARGV[5] ~= "" then
                    redis.call("hset", KEYS[4], ARGV[2], ARGV[5])
                end
                return 1
            """, Long.class);

    private final StringRedisTemplate redisTemplate;

    @Override
    public boolean isPublished(String deviceId) {
        return Boolean.TRUE.equals(redisTemplate.opsForSet().isMember(PUBLISHED_KEY, deviceId));
    }

    @Override
    public OptionalLong lastSnapshotAt(String deviceId) {
        Object value = redisTemplate.opsForHash().get(SNAPSHOT_AT_KEY, deviceId);
        return value == null ? OptionalLong.empty() : OptionalLong.of(Long.parseLong(value.toString()));
    }

    @Override
    public Optional<String> fingerprint(String deviceId) {
        return Optional.ofNullable(redisTemplate.opsForHash().get(FINGERPRINT_KEY, deviceId)).map(Object::toString);
    }

    @Override
    public PublicationMarkResult markPublication(String ownerToken, String deviceId, boolean published,
                                                 OptionalLong snapshotAt, String fingerprint) {
        Long result = redisTemplate.execute(MARK_PUBLICATION_SCRIPT,
                List.of(LEASE_KEY, PUBLISHED_KEY, SNAPSHOT_AT_KEY, FINGERPRINT_KEY),
                ownerToken, deviceId, published ? "1" : "0",
                snapshotAt.isPresent() ? Long.toString(snapshotAt.getAsLong()) : "",
                fingerprint == null ? "" : fingerprint);
        if (result == null) {
            throw new IllegalStateException("Redis returned no result for the publication mark script");
        }
        return switch (result.intValue()) {
            case -1 -> PublicationMarkResult.LEASE_LOST;
            case 0 -> PublicationMarkResult.STALE;
            case 1 -> PublicationMarkResult.WRITTEN;
            default -> throw new IllegalStateException("Unexpected publication mark script result: " + result);
        };
    }
}
