package com.github.gluhov.iotplatform.eventscollector.it;

import com.github.gluhov.iotplatform.eventscollector.adapter.out.redis.RedisLeaseService;
import com.github.gluhov.iotplatform.eventscollector.adapter.out.redis.RedisPublishedDeviceStore;
import com.github.gluhov.iotplatform.eventscollector.domain.model.PublicationMarkResult;
import org.junit.jupiter.api.*;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.OptionalLong;

import static com.github.gluhov.iotplatform.eventscollector.adapter.out.redis.RedisLeaseService.LEASE_KEY;
import static com.github.gluhov.iotplatform.eventscollector.adapter.out.redis.RedisPublishedDeviceStore.FINGERPRINT_KEY;
import static com.github.gluhov.iotplatform.eventscollector.adapter.out.redis.RedisPublishedDeviceStore.PUBLISHED_KEY;
import static com.github.gluhov.iotplatform.eventscollector.adapter.out.redis.RedisPublishedDeviceStore.SNAPSHOT_AT_KEY;
import static com.github.gluhov.iotplatform.eventscollector.config.RedisTestContainerConfig.PORT;
import static com.github.gluhov.iotplatform.eventscollector.config.RedisTestContainerConfig.REDIS_CONTAINER;
import static org.assertj.core.api.Assertions.assertThat;

// Lua-скрипт отметок публикации проверяется на настоящем Redis: мок скрипт не выполняет
class ItRedisPublishedDeviceStoreTest {
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final String DEVICE_ID = "dev-1";
    private static final String OLD_FINGERPRINT = "fingerprint-of-the-old-data";
    private static final String NEW_FINGERPRINT = "fingerprint-of-the-new-data";

    private LettuceConnectionFactory connectionFactory;
    private StringRedisTemplate redisTemplate;
    private RedisLeaseService leaseService;
    private RedisPublishedDeviceStore store;
    private String owner;

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
        clean();
        leaseService = new RedisLeaseService(redisTemplate);
        store = new RedisPublishedDeviceStore(redisTemplate);
        owner = leaseService.tryAcquire(TTL).orElseThrow();
    }

    @AfterEach
    void tearDown() {
        clean();
        connectionFactory.destroy();
    }

    private void clean() {
        redisTemplate.delete(java.util.List.of(LEASE_KEY, PUBLISHED_KEY, SNAPSHOT_AT_KEY, FINGERPRINT_KEY));
    }

    @Test
    @DisplayName("Owner marks a new device then published, snapshot time and fingerprint are written together")
    void givenOwner_whenMarkPublication_thenAllWritten() {
        PublicationMarkResult result = store.markPublication(owner, DEVICE_ID, true, OptionalLong.of(1_000L), NEW_FINGERPRINT);

        assertThat(result).isEqualTo(PublicationMarkResult.WRITTEN);
        assertThat(redisTemplate.opsForSet().isMember(PUBLISHED_KEY, DEVICE_ID)).isTrue();
        assertThat(redisTemplate.opsForHash().get(SNAPSHOT_AT_KEY, DEVICE_ID)).isEqualTo("1000");
        assertThat(redisTemplate.opsForHash().get(FINGERPRINT_KEY, DEVICE_ID)).isEqualTo(NEW_FINGERPRINT);
    }

    @Test
    @DisplayName("Telemetry without a snapshot then only the published mark")
    void givenNoSnapshot_whenMarkPublication_thenOnlyPublishedMark() {
        PublicationMarkResult result = store.markPublication(owner, DEVICE_ID, true, OptionalLong.empty(), null);

        assertThat(result).isEqualTo(PublicationMarkResult.WRITTEN);
        assertThat(redisTemplate.opsForSet().isMember(PUBLISHED_KEY, DEVICE_ID)).isTrue();
        assertThat(redisTemplate.opsForHash().hasKey(SNAPSHOT_AT_KEY, DEVICE_ID)).isFalse();
        assertThat(redisTemplate.opsForHash().hasKey(FINGERPRINT_KEY, DEVICE_ID)).isFalse();
    }

    @Test
    @DisplayName("Lease taken by another instance then nothing written, the new owner's state is kept")
    void givenLeaseTakenByAnother_whenMarkPublication_thenNothingWrittenAndForeignStateKept() {
        // Новый владелец уже сохранил более свежее состояние устройства
        redisTemplate.opsForValue().set(LEASE_KEY, "other-instance-token", TTL);
        redisTemplate.opsForSet().add(PUBLISHED_KEY, DEVICE_ID);
        redisTemplate.opsForHash().put(SNAPSHOT_AT_KEY, DEVICE_ID, "5000");
        redisTemplate.opsForHash().put(FINGERPRINT_KEY, DEVICE_ID, OLD_FINGERPRINT);

        PublicationMarkResult result = store.markPublication(owner, DEVICE_ID, true, OptionalLong.of(1_000L), NEW_FINGERPRINT);

        assertThat(result).isEqualTo(PublicationMarkResult.LEASE_LOST);
        assertThat(redisTemplate.opsForHash().get(SNAPSHOT_AT_KEY, DEVICE_ID)).isEqualTo("5000");
        assertThat(redisTemplate.opsForHash().get(FINGERPRINT_KEY, DEVICE_ID)).isEqualTo(OLD_FINGERPRINT);
        assertThat(redisTemplate.opsForValue().get(LEASE_KEY)).isEqualTo("other-instance-token");
    }

    @Test
    @DisplayName("Lease expired and nobody took it then nothing written for a new device")
    void givenExpiredLease_whenMarkPublication_thenNothingWritten() {
        redisTemplate.delete(LEASE_KEY);

        PublicationMarkResult result = store.markPublication(owner, DEVICE_ID, true, OptionalLong.of(1_000L), NEW_FINGERPRINT);

        assertThat(result).isEqualTo(PublicationMarkResult.LEASE_LOST);
        assertThat(redisTemplate.opsForSet().isMember(PUBLISHED_KEY, DEVICE_ID)).isFalse();
        assertThat(redisTemplate.opsForHash().hasKey(SNAPSHOT_AT_KEY, DEVICE_ID)).isFalse();
        assertThat(redisTemplate.opsForHash().hasKey(FINGERPRINT_KEY, DEVICE_ID)).isFalse();
        assertThat(redisTemplate.hasKey(LEASE_KEY)).as("the script does not recreate the lease").isFalse();
    }

    @Test
    @DisplayName("Older snapshot than the stored one then snapshot time and fingerprint are not rolled back")
    void givenOlderSnapshot_whenMarkPublication_thenStateNotRolledBack() {
        redisTemplate.opsForHash().put(SNAPSHOT_AT_KEY, DEVICE_ID, "5000");
        redisTemplate.opsForHash().put(FINGERPRINT_KEY, DEVICE_ID, NEW_FINGERPRINT);

        PublicationMarkResult result = store.markPublication(owner, DEVICE_ID, true, OptionalLong.of(1_000L), OLD_FINGERPRINT);

        assertThat(result).isEqualTo(PublicationMarkResult.STALE);
        assertThat(redisTemplate.opsForHash().get(SNAPSHOT_AT_KEY, DEVICE_ID)).isEqualTo("5000");
        assertThat(redisTemplate.opsForHash().get(FINGERPRINT_KEY, DEVICE_ID)).isEqualTo(NEW_FINGERPRINT);
        assertThat(redisTemplate.opsForSet().isMember(PUBLISHED_KEY, DEVICE_ID)).as("published mark is idempotent").isTrue();
    }

    @Test
    @DisplayName("Same snapshot time as the stored one then written: a republished record may refresh the fingerprint")
    void givenSameSnapshotTime_whenMarkPublication_thenWritten() {
        redisTemplate.opsForHash().put(SNAPSHOT_AT_KEY, DEVICE_ID, "1000");
        redisTemplate.opsForHash().put(FINGERPRINT_KEY, DEVICE_ID, OLD_FINGERPRINT);

        PublicationMarkResult result = store.markPublication(owner, DEVICE_ID, true, OptionalLong.of(1_000L), NEW_FINGERPRINT);

        assertThat(result).isEqualTo(PublicationMarkResult.WRITTEN);
        assertThat(redisTemplate.opsForHash().get(FINGERPRINT_KEY, DEVICE_ID)).isEqualTo(NEW_FINGERPRINT);
    }

    @Test
    @DisplayName("Newer snapshot than the stored one then both advance")
    void givenNewerSnapshot_whenMarkPublication_thenAdvanced() {
        redisTemplate.opsForHash().put(SNAPSHOT_AT_KEY, DEVICE_ID, "1000");
        redisTemplate.opsForHash().put(FINGERPRINT_KEY, DEVICE_ID, OLD_FINGERPRINT);

        PublicationMarkResult result = store.markPublication(owner, DEVICE_ID, true, OptionalLong.of(2_000L), NEW_FINGERPRINT);

        assertThat(result).isEqualTo(PublicationMarkResult.WRITTEN);
        assertThat(redisTemplate.opsForHash().get(SNAPSHOT_AT_KEY, DEVICE_ID)).isEqualTo("2000");
        assertThat(redisTemplate.opsForHash().get(FINGERPRINT_KEY, DEVICE_ID)).isEqualTo(NEW_FINGERPRINT);
    }

    @Test
    @DisplayName("Skipped snapshot (no published mark) then only the snapshot time moves, the fingerprint stays")
    void givenSkippedSnapshot_whenMarkPublication_thenOnlySnapshotTimeAdvanced() {
        redisTemplate.opsForHash().put(SNAPSHOT_AT_KEY, DEVICE_ID, "1000");
        redisTemplate.opsForHash().put(FINGERPRINT_KEY, DEVICE_ID, OLD_FINGERPRINT);

        PublicationMarkResult result = store.markPublication(owner, DEVICE_ID, false, OptionalLong.of(2_000L), null);

        assertThat(result).isEqualTo(PublicationMarkResult.WRITTEN);
        assertThat(redisTemplate.opsForHash().get(SNAPSHOT_AT_KEY, DEVICE_ID)).isEqualTo("2000");
        assertThat(redisTemplate.opsForHash().get(FINGERPRINT_KEY, DEVICE_ID)).isEqualTo(OLD_FINGERPRINT);
        assertThat(redisTemplate.opsForSet().isMember(PUBLISHED_KEY, DEVICE_ID)).isFalse();
    }
}