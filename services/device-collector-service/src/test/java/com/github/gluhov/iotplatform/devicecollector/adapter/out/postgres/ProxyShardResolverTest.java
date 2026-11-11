package com.github.gluhov.iotplatform.devicecollector.adapter.out.postgres;

import com.github.gluhov.iotplatform.devicecollector.config.ProcessingProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ProxyShardResolverTest {
    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final MutableClock clock = new MutableClock(Instant.parse("2026-11-03T10:00:00Z"));
    private final ProxyShardResolver resolver = new ProxyShardResolver(jdbc, clock,
            new ProcessingProperties(3, Duration.ofSeconds(1), 2.0, Duration.ofDays(1), Duration.ofMinutes(5)));

    @Test
    @DisplayName("Shard comes from PREVIEW with a literal (Proxy ignores PREVIEW parameters) and is cached for TTL")
    void givenPreviewAnswer_whenShardOf_thenCachedUntilTtl() {
        when(jdbc.query(anyString(), any(ResultSetExtractor.class))).thenReturn("shard_1");

        assertThat(resolver.shardOf("dev-1")).isEqualTo("shard_1");
        assertThat(resolver.shardOf("dev-1")).isEqualTo("shard_1");
        verify(jdbc, times(1)).query(sql().capture(), any(ResultSetExtractor.class));

        clock.advance(Duration.ofMinutes(6));
        resolver.shardOf("dev-1");
        ArgumentCaptor<String> sql = sql();
        verify(jdbc, times(2)).query(sql.capture(), any(ResultSetExtractor.class));
        assertThat(sql.getValue()).isEqualTo("PREVIEW SELECT 1 FROM devices WHERE device_id = 'dev-1'");
    }

    @Test
    @DisplayName("Proxy does not answer: shard is unknown and nothing is cached")
    void givenPreviewFailure_whenShardOf_thenUnknown() {
        when(jdbc.query(anyString(), any(ResultSetExtractor.class)))
                .thenThrow(new DataAccessResourceFailureException("Connection is not available"))
                .thenReturn("shard_0");

        assertThat(resolver.shardOf("dev-1")).isEqualTo("unknown");
        assertThat(resolver.shardOf("dev-1")).isEqualTo("shard_0");
    }

    @Test
    @DisplayName("Literal: quotes and backslashes go into dollar quotes; both quote kinds - no PREVIEW at all")
    void givenSpecialCharacters_whenSqlLiteral_thenSafeLiteralOrNone() {
        assertThat(ProxyShardResolver.sqlLiteral("dev-1")).isEqualTo("'dev-1'");
        assertThat(ProxyShardResolver.sqlLiteral("o'neil")).isEqualTo("$$o'neil$$");
        assertThat(ProxyShardResolver.sqlLiteral("a\\b")).isEqualTo("$$a\\b$$");
        assertThat(ProxyShardResolver.sqlLiteral("a$b")).isEqualTo("'a$b'");
        assertThat(ProxyShardResolver.sqlLiteral("o'$neil")).isNull();

        assertThat(resolver.shardOf("o'$neil")).isEqualTo("unknown");
        verify(jdbc, never()).query(anyString(), any(ResultSetExtractor.class));
    }

    private static ArgumentCaptor<String> sql() {
        return ArgumentCaptor.forClass(String.class);
    }

    private static final class MutableClock extends Clock {
        private Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void advance(Duration duration) {
            now = now.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}