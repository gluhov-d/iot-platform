package com.github.gluhov.iotplatform.eventscollector.it;

import com.github.gluhov.iotplatform.eventscollector.adapter.out.redis.RedisLeaseService;
import org.junit.jupiter.api.*;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.github.gluhov.iotplatform.eventscollector.adapter.out.redis.RedisLeaseService.LEASE_KEY;
import static com.github.gluhov.iotplatform.eventscollector.config.RedisTestContainerConfig.PORT;
import static com.github.gluhov.iotplatform.eventscollector.config.RedisTestContainerConfig.REDIS_CONTAINER;
import static org.assertj.core.api.Assertions.assertThat;

// Lua-скрипт освобождения аренды проверяется на настоящем Redis: мок скрипт не выполняет
class ItRedisLeaseServiceTest {
    private static final Duration TTL = Duration.ofSeconds(10);
    private static final Duration SHORT_TTL = Duration.ofMillis(200);

    private LettuceConnectionFactory connectionFactory;
    private StringRedisTemplate redisTemplate;
    private RedisLeaseService leaseService;

    @BeforeAll
    static void startRedis() {
        if (!REDIS_CONTAINER.isRunning()) {
            REDIS_CONTAINER.start();
        }
    }

    @BeforeEach
    void setUp() {
        connectionFactory = new LettuceConnectionFactory(REDIS_CONTAINER.getHost(), REDIS_CONTAINER.getMappedPort(PORT));
        connectionFactory.afterPropertiesSet();
        connectionFactory.start();
        redisTemplate = new StringRedisTemplate(connectionFactory);
        redisTemplate.delete(LEASE_KEY);
        leaseService = new RedisLeaseService(redisTemplate);
    }

    @AfterEach
    void tearDown() {
        redisTemplate.delete(LEASE_KEY);
        connectionFactory.destroy();
    }

    @Test
    @DisplayName("Lease held then second acquire fails and key has owner token with TTL")
    void givenHeldLease_whenAcquireAgain_thenFails() {
        Optional<String> first = leaseService.tryAcquire(TTL);
        Optional<String> second = leaseService.tryAcquire(TTL);

        assertThat(first).isPresent();
        assertThat(second).isEmpty();
        assertThat(redisTemplate.opsForValue().get(LEASE_KEY)).isEqualTo(first.orElseThrow());
        assertThat(redisTemplate.getExpire(LEASE_KEY)).isBetween(1L, 10L);
    }

    @Test
    @DisplayName("Release by owner then key deleted")
    void givenOwnerToken_whenRelease_thenKeyDeleted() {
        String token = leaseService.tryAcquire(TTL).orElseThrow();

        assertThat(leaseService.release(token)).isTrue();
        assertThat(redisTemplate.hasKey(LEASE_KEY)).isFalse();
    }

    @Test
    @DisplayName("Release with stale token after lease taken by another instance then key kept")
    void givenStaleToken_whenRelease_thenForeignLeaseKept() throws InterruptedException {
        String stale = leaseService.tryAcquire(SHORT_TTL).orElseThrow();
        Thread.sleep(SHORT_TTL.multipliedBy(2));
        String current = leaseService.tryAcquire(TTL).orElseThrow();

        assertThat(leaseService.release(stale)).isFalse();
        assertThat(redisTemplate.opsForValue().get(LEASE_KEY)).isEqualTo(current);
    }

    @Test
    @DisplayName("Lease TTL expires then lease can be acquired again")
    void givenExpiredLease_whenAcquire_thenSucceeds() throws InterruptedException {
        leaseService.tryAcquire(SHORT_TTL).orElseThrow();
        Thread.sleep(SHORT_TTL.multipliedBy(2));

        assertThat(leaseService.tryAcquire(Duration.ofSeconds(1))).isPresent();
    }

    @Test
    @DisplayName("Extend by owner then TTL prolonged")
    void givenOwnerToken_whenExtend_thenTtlProlonged() {
        String token = leaseService.tryAcquire(Duration.ofSeconds(2)).orElseThrow();

        assertThat(leaseService.extend(token, TTL)).isTrue();
        assertThat(redisTemplate.getExpire(LEASE_KEY, TimeUnit.MILLISECONDS)).isGreaterThan(2_000L);
        assertThat(redisTemplate.opsForValue().get(LEASE_KEY)).isEqualTo(token);
    }

    @Test
    @DisplayName("Extend with stale token after lease taken by another instance then foreign lease untouched")
    void givenStaleToken_whenExtend_thenForeignLeaseUntouched() throws InterruptedException {
        String stale = leaseService.tryAcquire(SHORT_TTL).orElseThrow();
        Thread.sleep(SHORT_TTL.multipliedBy(2));
        String current = leaseService.tryAcquire(Duration.ofSeconds(2)).orElseThrow();

        assertThat(leaseService.extend(stale, TTL)).isFalse();
        assertThat(redisTemplate.opsForValue().get(LEASE_KEY)).isEqualTo(current);
        assertThat(redisTemplate.getExpire(LEASE_KEY, TimeUnit.MILLISECONDS)).isLessThanOrEqualTo(2_000L);
    }

    @Test
    @DisplayName("Extend after lease expired then key not recreated")
    void givenExpiredLease_whenExtend_thenFalse() throws InterruptedException {
        String token = leaseService.tryAcquire(SHORT_TTL).orElseThrow();
        Thread.sleep(SHORT_TTL.multipliedBy(2));

        assertThat(leaseService.extend(token, TTL)).isFalse();
        assertThat(redisTemplate.hasKey(LEASE_KEY)).isFalse();
    }
}
