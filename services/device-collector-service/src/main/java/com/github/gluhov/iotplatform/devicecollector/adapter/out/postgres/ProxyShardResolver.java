package com.github.gluhov.iotplatform.devicecollector.adapter.out.postgres;

import com.github.gluhov.iotplatform.devicecollector.config.ProcessingProperties;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessingResult;
import com.github.gluhov.iotplatform.devicecollector.domain.port.out.ShardResolver;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Шард устройства на пути ошибки, когда ответа шарда нет: PREVIEW - маршрут, который построил бы Proxy;
 * Proxy отвечает сам, к шарду не обращается (проверено при остановленном шарде). Результат кешируется:
 * при недоступном шарде повторы одного устройства не повторяют запрос. Proxy не ответил - "unknown".
 */
@Component
@Slf4j
public class ProxyShardResolver implements ShardResolver {
    static final int MAX_CACHED_DEVICES = 10_000;

    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final Duration ttl;
    private final Map<String, Cached> cache = new ConcurrentHashMap<>();

    public ProxyShardResolver(JdbcTemplate jdbc, Clock clock, ProcessingProperties properties) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.ttl = properties.shardCacheTtl();
    }

    @Override
    public String shardOf(String deviceId) {
        Instant now = clock.instant();
        Cached cached = cache.get(deviceId);
        if (cached != null && now.isBefore(cached.expiresAt())) {
            return cached.shard();
        }
        String literal = sqlLiteral(deviceId);
        if (literal == null) {
            return ProcessingResult.UNKNOWN_SHARD;
        }
        try {
            // Statement, а не prepared statement: параметр PREVIEW Proxy 5.5.3 не учитывает и всегда отвечает shard_0
            String shard = jdbc.query("PREVIEW SELECT 1 FROM devices WHERE device_id = " + literal,
                    rs -> rs.next() ? rs.getString("data_source_name") : null);
            if (shard == null) {
                return ProcessingResult.UNKNOWN_SHARD;
            }
            if (cache.size() >= MAX_CACHED_DEVICES) {
                cache.clear();
            }
            cache.put(deviceId, new Cached(shard, now.plus(ttl)));
            return shard;
        } catch (DataAccessException e) {
            log.warn("Shard of device {} not resolved by PREVIEW: {}", deviceId, e.getMostSpecificCause().getMessage());
            return ProcessingResult.UNKNOWN_SHARD;
        }
    }

    /**
     * Строковый литерал для PREVIEW или null, если его нельзя построить надёжно. Значение с кавычкой или
     * обратной косой чертой - в долларовых кавычках: экранированную кавычку ('o''neil') Proxy 5.5.3
     * маршрутизирует как текст с двумя кавычками, то есть на другой шард.
     */
    static String sqlLiteral(String value) {
        if (value.indexOf('\'') < 0 && value.indexOf('\\') < 0) {
            return "'" + value + "'";
        }
        if (value.indexOf('$') < 0) {
            return "$$" + value + "$$";
        }
        return null;
    }

    private record Cached(String shard, Instant expiresAt) {}
}