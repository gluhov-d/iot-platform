package com.github.gluhov.iotplatform.devicecollector.it;

import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import com.github.gluhov.iotplatform.events.avro.OperationType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.github.gluhov.iotplatform.devicecollector.data.TestData.adminSnapshot;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.command;
import static com.github.gluhov.iotplatform.devicecollector.data.TestData.info;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Команды администратора и сброс кэша на стенде из AbstractItTest: Kafka, Schema Registry, два шарда и Proxy, Redis.
 * Время событий - в прошлом относительно часов сервиса (будущее время отклоняет валидатор).
 */
class ItDeviceOperationsTest extends AbstractItTest {
    private static final Instant T = Instant.now().minus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MILLIS);

    @Autowired
    private StringRedisTemplate redis;

    // ItDeviceCollectorTest останавливает Proxy последним тестом: контейнеры общие на JVM, поднимаем обратно
    @BeforeAll
    static void proxyReady() {
        ensureProxyReady();
    }

    @Test
    @DisplayName("FR-015: CREATE, UPDATE, DELETE applied in order; version grows by one per applied command")
    void givenCreateUpdateDelete_whenConsumed_thenApplied() throws Exception {
        String id = deviceOnShard(0);
        send(id, command(id, "cmd-1", OperationType.CREATE, T).build(), null);
        send(id, command(id, "cmd-2", OperationType.UPDATE, T.plusSeconds(1))
                .setDevice(adminSnapshot(id).setName("Renamed").build()).build(), null);
        RecordMetadata last = send(id, command(id, "cmd-3", OperationType.DELETE, T.plusSeconds(2)).build(), null);

        awaitCommitted(last);
        int shard = assertOnlyOnExpectedShard(id);
        assertThat(onShard(shard, "SELECT version, status, deleted_by, name FROM devices WHERE device_id = '" + id + "'"))
                .isEqualTo("3|DELETED|ADMIN|Renamed");
        assertThat(onShard(shard, "SELECT string_agg(event_key || ':' || outcome, ',' ORDER BY event_key) "
                + "FROM processed_device_events WHERE device_id = '" + id + "'"))
                .isEqualTo("cmd-1:APPLIED,cmd-2:APPLIED,cmd-3:APPLIED");
    }

    @Test
    @DisplayName("FR-016: a router event does not bring back a device deleted by the administrator")
    void givenAdminDelete_whenRouterEventArrives_thenStillDeleted() throws Exception {
        String id = deviceOnShard(1);
        send(id, command(id, "cmd-1", OperationType.DELETE, T).build(), null);
        RecordMetadata router = send(id, info(id, "evt-1", DeviceUpsertEventType.UPDATED, T.plusSeconds(10)).build(), null);

        awaitCommitted(router);
        int shard = assertOnlyOnExpectedShard(id);
        assertThat(onShard(shard, "SELECT status, deleted_by FROM devices WHERE device_id = '" + id + "'"))
                .isEqualTo("DELETED|ADMIN");
        assertThat(onShard(shard, "SELECT outcome FROM processed_device_events WHERE device_id = '" + id
                + "' AND event_key = 'evt-1'")).isEqualTo("IGNORED");
    }

    @Test
    @DisplayName("FR-015: the same command delivered twice is applied once")
    void givenSameCommandTwice_whenConsumed_thenAppliedOnce() throws Exception {
        String id = deviceOnShard(0);
        DeviceInfo command = command(id, "cmd-1", OperationType.CREATE, T).build();
        send(id, command, null);
        RecordMetadata repeated = send(id, command, null);

        awaitCommitted(repeated);
        int shard = assertOnlyOnExpectedShard(id);
        assertThat(onShard(shard, "SELECT version, seen_count FROM devices WHERE device_id = '" + id + "'"))
                .isEqualTo("1|1");
        assertThat(onShard(shard, "SELECT count(*) FROM processed_device_events WHERE device_id = '" + id + "'"))
                .isEqualTo("1");
    }

    @Test
    @DisplayName("FR-015, строка 11: command UNKNOWN goes to devices-dlt, the next message of the partition is processed")
    void givenUnknownOperation_whenConsumed_thenDeadLettered() throws Exception {
        String id = deviceOnShard(1);
        RecordMetadata unknown = send(id, command(id, "cmd-1", OperationType.DELETE, T)
                .setOperation(OperationType.UNKNOWN).build(), null);
        RecordMetadata next = send(id, info(id, "evt-1", DeviceUpsertEventType.CREATED, T).build(), null);

        awaitCommitted(next);
        assertThat(assertOnlyOnExpectedShard(id)).isEqualTo(1);
        ConsumerRecord<String, byte[]> dead = deadLetter(unknown).orElseThrow();
        assertThat(header(dead, "kafka_dlt-exception-cause-fqcn")).endsWith("InvalidDeviceInfoException");
    }

    @Test
    @DisplayName("FR-018: after an applied update the cached copy older than the applied version is evicted")
    void givenCachedDevice_whenUpdateConsumed_thenCacheEvicted() throws Exception {
        String id = deviceOnShard(0);
        awaitCommitted(send(id, command(id, "cmd-1", OperationType.CREATE, T).build(), null));
        String key = "device:v1:" + id;
        String floorKey = "device:v1:floor:" + id;
        // Как device-service при промахе кэша: тело и версия записи, прочитанной с реплики, со сроком кэша
        redis.opsForHash().putAll(key, Map.of("body", "{\"version\":1}", "version", "1"));
        redis.expire(key, Duration.ofMinutes(10));
        RecordMetadata update = send(id, command(id, "cmd-2", OperationType.UPDATE, T.plusSeconds(1)).build(), null);

        awaitCommitted(update);
        assertThat(redis.opsForValue().get(floorKey)).isEqualTo("2");
        assertThat(redis.opsForHash().hasKey(key, "body")).isFalse();
        assertThat(redis.opsForHash().hasKey(key, "version")).isFalse();
        // Порог живёт дольше кэша (DEVICE_CACHE_FLOOR_TTL = 24 ч из application-test.yaml): отставание реплики
        // дольше срока кэша не вернёт старые данные (ADR-0003)
        Long floorTtl = redis.getExpire(floorKey, TimeUnit.MILLISECONDS);
        assertThat(floorTtl).isNotNull().isGreaterThan(TimeUnit.MINUTES.toMillis(10));
    }

    @Test
    @DisplayName("FR-022: тема devices не сжимается - команда и более позднее событие роутера остаются по порядку")
    void givenAdminDeleteThenRouterEvent_whenTopicReadFromBeginning_thenBothKeptInOrder() throws Exception {
        String id = deviceOnShard(0);
        send(id, command(id, "cmd-1", OperationType.DELETE, T).build(), null);
        RecordMetadata router = send(id, info(id, "evt-1", DeviceUpsertEventType.UPDATED, T.plusSeconds(10)).build(), null);
        awaitCommitted(router);

        assertThat(topicConfig(DEVICES, "cleanup.policy")).isEqualTo("delete");
        assertThat(topicConfig(DEVICES, "retention.ms")).isEqualTo("2592000000");
        // Отставший обработчик читает оба сообщения в исходном порядке: со сжатием осталось бы только событие роутера
        assertThat(messagesOf(DEVICES, id)).hasSize(2)
                .extracting(record -> record.offset())
                .isSorted();
        int shard = assertOnlyOnExpectedShard(id);
        assertThat(onShard(shard, "SELECT status, deleted_by FROM devices WHERE device_id = '" + id + "'"))
                .isEqualTo("DELETED|ADMIN");
    }

    @Test
    @DisplayName("FR-021: Redis stopped - messages are still applied and acknowledged")
    void givenRedisDown_whenConsumed_thenApplied() throws Exception {
        String id = deviceOnShard(1);
        pauseShard(REDIS);
        try {
            awaitCommitted(send(id, command(id, "cmd-1", OperationType.CREATE, T).build(), null));
        } finally {
            unpauseShard(REDIS);
        }

        int shard = assertOnlyOnExpectedShard(id);
        assertThat(onShard(shard, "SELECT version FROM devices WHERE device_id = '" + id + "'")).isEqualTo("1");
    }
}
