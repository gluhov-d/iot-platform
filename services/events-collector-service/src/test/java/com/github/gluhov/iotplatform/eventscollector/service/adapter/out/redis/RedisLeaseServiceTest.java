package com.github.gluhov.iotplatform.eventscollector.service.adapter.out.redis;

import com.github.gluhov.iotplatform.eventscollector.adapter.out.redis.RedisLeaseService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static com.github.gluhov.iotplatform.eventscollector.adapter.out.redis.RedisLeaseService.LEASE_KEY;
import static com.github.gluhov.iotplatform.eventscollector.adapter.out.redis.RedisLeaseService.RELEASE_SCRIPT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RedisLeaseServiceTest {
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final String TOKEN = "owner-token";

    @Mock
    private StringRedisTemplate redisTemplate;
    @InjectMocks
    private RedisLeaseService leaseService;

    @Test
    @DisplayName("Lease key is free then lease acquired with random token")
    @SuppressWarnings("unchecked")
    void givenFreeLease_whenTryAcquire_thenReturnToken() {
        when(redisTemplate.execute(any(RedisCallback.class))).thenReturn(Boolean.TRUE);

        Optional<String> first = leaseService.tryAcquire(TTL);
        Optional<String> second = leaseService.tryAcquire(TTL);

        assertThat(first).isPresent();
        assertThat(second).isPresent();
        assertThat(first.get()).isNotEqualTo(second.get());
    }

    @Test
    @DisplayName("Lease held by another instance then not acquired")
    @SuppressWarnings("unchecked")
    void givenHeldLease_whenTryAcquire_thenEmpty() {
        when(redisTemplate.execute(any(RedisCallback.class))).thenReturn(Boolean.FALSE);

        assertThat(leaseService.tryAcquire(TTL)).isEmpty();
    }

    @Test
    @DisplayName("Release by owner then key deleted")
    void givenOwnerToken_whenRelease_thenKeyDeleted() {
        when(redisTemplate.execute(RELEASE_SCRIPT, List.of(LEASE_KEY), TOKEN)).thenReturn(1L);

        assertThat(leaseService.release(TOKEN)).isTrue();
        verify(redisTemplate).execute(RELEASE_SCRIPT, List.of(LEASE_KEY), TOKEN);
    }

    @Test
    @DisplayName("Release with foreign token then key kept")
    void givenForeignToken_whenRelease_thenKeyKept() {
        when(redisTemplate.execute(RELEASE_SCRIPT, List.of(LEASE_KEY), "foreign-token")).thenReturn(0L);

        assertThat(leaseService.release("foreign-token")).isFalse();
    }
}
