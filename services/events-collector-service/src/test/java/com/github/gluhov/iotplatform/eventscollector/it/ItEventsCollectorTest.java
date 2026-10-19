package com.github.gluhov.iotplatform.eventscollector.it;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.eventscollector.adapter.out.kafka.DeviceInfoPublishException;
import com.github.gluhov.iotplatform.eventscollector.domain.model.BatchResult;
import com.github.gluhov.iotplatform.eventscollector.domain.port.in.PublishOutboxUseCase;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.DeviceInfoPublisher;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.OutboxRepository;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static com.github.gluhov.iotplatform.eventscollector.adapter.out.redis.RedisLeaseService.LEASE_KEY;
import static com.github.gluhov.iotplatform.eventscollector.adapter.out.redis.RedisPublishedDeviceStore.PUBLISHED_KEY;
import static com.github.gluhov.iotplatform.eventscollector.config.SchemaRegistryTestContainerConfig.schemaRegistryUrl;
import static com.github.gluhov.iotplatform.eventscollector.data.TestData.EVENT_TYPE;
import static com.github.gluhov.iotplatform.eventscollector.data.TestData.unique;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ItEventsCollectorTest extends AbstractItTest {
    private static final Duration WAIT = Duration.ofSeconds(30);
    private static final String EVENTS_TOPIC = "events";
    private static final String DEVICES_TOPIC = "devices";
    
    private static KafkaProducer<String, DeviceEvent> producer;
    private static KafkaConsumer<String, DeviceInfo> deviceInfoKafkaConsumer;
    private static final List<ConsumerRecord<String, DeviceInfo>> devicesSeen = new ArrayList<>();
    
    @Autowired
    private JdbcClient jdbcClient;
    @Autowired
    private StringRedisTemplate redisTemplate;
    @Autowired
    private PublishOutboxUseCase publishOutbox;
    @MockitoSpyBean
    private DeviceInfoPublisher deviceInfoPublisher;
    @MockitoSpyBean
    private OutboxRepository outboxRepository;
    
    @BeforeAll
    static void createClients() {
        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class,
                "schema.registry.url", schemaRegistryUrl()));
        deviceInfoKafkaConsumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "devices-it-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class,
                "schema.registry.url", schemaRegistryUrl(),
                "specific.avro.reader", true));
        deviceInfoKafkaConsumer.subscribe(List.of(DEVICES_TOPIC));
    }
    
    @AfterAll
    static void closeClients() {
        producer.close();
        deviceInfoKafkaConsumer.close();
    }
    
    @Test
    @DisplayName("Send event then row in device_events and initial outbox record")
    void givenEvent_whenIngest_thenStoredWithInitialOutbox() {
        String eventId = unique("evt");
        String deviceId = unique("dev");
        long timestampMs = Instant.now().toEpochMilli();
        
        send(eventId, deviceId, timestampMs);
        
        await().atMost(WAIT).untilAsserted(() -> assertThat(eventRows(eventId)).isEqualTo(1));
        assertThat(outboxRow(eventId))
                .containsEntry("device_id", deviceId)
                .containsEntry("status", "NEW")
                .containsEntry("state_version", 1L)
                .containsEntry("attempts", 0L)
                .containsEntry("source_timestamp_ms", timestampMs);
    }
    
    @Test
    @DisplayName("Publish outbox then DeviceInfo in devices, SENT version and lease released")
    void givenNewDevice_whenPublish_thenDeviceInfoSentAndSecondEventSkipped() {
        String eventId = unique("evt");
        String deviceId = unique("dev");
        long timestampMs = Instant.now().toEpochMilli();
        
        send(eventId, deviceId, timestampMs);
        
        awaitOutbox(eventId);

        BatchResult result = publishOutbox.publishPendingBatch();
        
        assertThat(result.leaseAcquired()).isTrue();
        assertThat(outboxRow(eventId))
                .containsEntry("status", "SENT")
                .containsEntry("state_version", 2L);
        assertThat(redisTemplate.opsForSet().isMember(PUBLISHED_KEY, deviceId)).isTrue();
        await().atMost(WAIT).untilAsserted(() -> assertThat(publishedDeviceInfo(deviceId)).isNotEmpty());
        assertThat(redisTemplate.hasKey(LEASE_KEY)).as("lease released").isFalse();
        assertThat(auditTrail(eventId)).containsExactly("PUBLISHED/1/it");
        // Сквозной трейс: DeviceInfo отправлен в том же трейсе, в котором событие было принято
        String ingestionTraceId = String.valueOf(outboxRow(eventId).get("trace_id"));
        assertThat(ingestionTraceId).hasSize(32);
        assertThat(publishedTraceIds(deviceId)).containsExactly(ingestionTraceId);
        
        // Второе событие того же устройства: публикации нет, только версия SENT
        String secondEventId = unique("evt");
        send(secondEventId, deviceId, Instant.now().toEpochMilli());
        awaitOutbox(secondEventId);
        publishOutbox.publishPendingBatch();
        
        assertThat(outboxRow(secondEventId)).containsEntry("status", "SENT");
        assertThat(auditTrail(secondEventId)).containsExactly("SKIPPED/1/it");
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(publishedDeviceInfo(deviceId)).hasSize(1));
    }

    @Test
    @DisplayName("Redeliver same eventId after publish then duplicate collapses and SENT kept")
    void givenRedeliveredEvent_whenIngest_thenDuplicateCollapsesAndSentKept() {
        String eventId = unique("evt");
        String deviceId = unique("dev");
        long timestampMs = Instant.now().toEpochMilli();

        send(eventId, deviceId, timestampMs);
        awaitOutbox(eventId);
        publishOutbox.publishPendingBatch();
        assertThat(outboxRow(eventId)).containsEntry("status", "SENT");

        // Маркер с тем же ключом попадает в тот же раздел и обрабатывается строго после повтора
        send(eventId, deviceId, timestampMs);
        String marker = unique("evt");
        send(marker, deviceId, timestampMs + 1);
        awaitOutbox(marker);

        assertThat(eventRows(eventId)).as("FINAL collapses the duplicate").isEqualTo(1);
        assertThat(outboxRow(eventId))
                .as("redelivered v1 NEW loses to v2 SENT")
                .containsEntry("status", "SENT")
                .containsEntry("state_version", 2L);
    }

    @Test
    @DisplayName("Publish fails then FAILED with attempts +1, retry succeeds, max attempts stop selection")
    void givenPublishFailure_whenRetry_thenSentOrStopsAfterMaxAttempts() {
        String retriedEvent = unique("evt");
        String retriedDevice = unique("dev");
        doThrow(new DeviceInfoPublishException("broker unavailable", null))
                .doCallRealMethod()
                .when(deviceInfoPublisher).publish(eq(retriedDevice), anyLong());

        send(retriedEvent, retriedDevice, Instant.now().toEpochMilli());
        awaitOutbox(retriedEvent);

        publishOutbox.publishPendingBatch();

        assertThat(outboxRow(retriedEvent))
                .containsEntry("status", "FAILED")
                .containsEntry("attempts", 1L)
                .containsEntry("state_version", 2L);
        assertThat(String.valueOf(outboxRow(retriedEvent).get("last_error"))).contains("broker unavailable");
        assertThat(redisTemplate.opsForSet().isMember(PUBLISHED_KEY, retriedDevice)).isFalse();

        publishOutbox.publishPendingBatch();

        assertThat(outboxRow(retriedEvent))
                .containsEntry("status", "SENT")
                .containsEntry("attempts", 1L)
                .containsEntry("state_version", 3L);
        await().atMost(WAIT).untilAsserted(() -> assertThat(publishedDeviceInfo(retriedDevice)).hasSize(1));
        assertThat(auditTrail(retriedEvent)).containsExactly("FAILED/1/it", "PUBLISHED/2/it");

        // Постоянная ошибка: после OUTBOX_MAX_ATTEMPTS (3 в тестах) запись больше не выбирается
        String brokenEvent = unique("evt");
        String brokenDevice = unique("dev");
        doThrow(new DeviceInfoPublishException("still broken", null))
                .when(deviceInfoPublisher).publish(eq(brokenDevice), anyLong());
        send(brokenEvent, brokenDevice, Instant.now().toEpochMilli());
        awaitOutbox(brokenEvent);

        for (int i = 0; i < 5; i++) {
            publishOutbox.publishPendingBatch();
        }
        assertThat(outboxRow(brokenEvent))
                .containsEntry("status", "FAILED")
                .containsEntry("attempts", 3L)
                .containsEntry("state_version", 4L);
        assertThat(auditTrail(brokenEvent)).containsExactly("FAILED/1/it", "FAILED/2/it", "FAILED/3/it");
    }

    private static void send(String eventId, String deviceId, long timestampMs) {
        DeviceEvent event = DeviceEvent.newBuilder()
                .setEventId(eventId)
                .setDeviceId(deviceId)
                .setTimestampMs(timestampMs)
                .setEventType(EVENT_TYPE)
                .build();
        
        try {
            producer.send(new ProducerRecord<>(EVENTS_TOPIC, deviceId, event)).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
    
    private void awaitOutbox(String eventId) {
        await().atMost(WAIT).untilAsserted(() -> assertThat(outboxRowCount(eventId)).isEqualTo(1));
    }
    
    private long eventRows(String eventId) {
        return jdbcClient.sql("SELECT count() FROM device_events FINAL WHERE event_id = :id")
                .param("id", eventId).query(Long.class).single();
    }
    
    private long outboxRowCount(String eventId) {
        return jdbcClient.sql("SELECT count() FROM device_outbox FINAL WHERE event_id = :id")
                .param("id", eventId).query(Long.class).single();
    }
    
    private Map<String, Object> outboxRow(String eventId) {
        return jdbcClient.sql("""
                        SELECT device_id, source_timestamp_ms, toString(status) AS status,
                               toInt64(state_version) AS state_version, toInt64(attempts) AS attempts, last_error, trace_id
                        FROM device_outbox FINAL WHERE event_id = :id
                        """)
                .param("id", eventId)
                .query((rs, rowNum) -> Map.<String, Object>of(
                        "device_id", rs.getString("device_id"),
                        "source_timestamp_ms", rs.getLong("source_timestamp_ms"),
                        "state_version", rs.getLong("state_version"),
                        "status", rs.getString("status"),
                        "attempts", rs.getLong("attempts"),
                        "last_error", rs.getString("last_error"),
                        "trace_id", rs.getString("trace_id")))
                .single();
    }

    @Test
    @DisplayName("Lease TTL expires during findPending and another instance takes it then nothing published or written")
    void givenLeaseExpiredDuringSelection_whenPublish_thenNothingPublishedOrWritten() {
        String eventId = unique("evt");
        String deviceId = unique("dev");
        String otherOwner = "other-instance-token";
        send(eventId, deviceId, Instant.now().toEpochMilli());
        awaitOutbox(eventId);
        // Выборка длится дольше TTL: аренда истекает в Redis, и ее получает другой экземпляр
        doAnswer(call -> {
            redisTemplate.expire(LEASE_KEY, Duration.ofMillis(1));
            await().atMost(Duration.ofSeconds(2)).until(() -> !redisTemplate.hasKey(LEASE_KEY));
            redisTemplate.opsForValue().setIfAbsent(LEASE_KEY, otherOwner, Duration.ofSeconds(30));
            return call.callRealMethod();
        }).when(outboxRepository).findPending(anyInt(), anyInt());

        try {
            BatchResult result = publishOutbox.publishPendingBatch();

            assertThat(result.leaseLost()).isTrue();
            assertThat(result.published()).isZero();
            verify(deviceInfoPublisher, never()).publish(eq(deviceId), anyLong());
            assertThat(outboxRow(eventId)).as("no new outbox version")
                    .containsEntry("status", "NEW")
                    .containsEntry("state_version", 1L);
            assertThat(auditTrail(eventId)).isEmpty();
            assertThat(redisTemplate.opsForValue().get(LEASE_KEY)).as("new owner's lease kept").isEqualTo(otherOwner);
        } finally {
            redisTemplate.delete(LEASE_KEY);
        }
    }

    // Попытки публикации события из аудита: "исход/номер попытки/экземпляр" в порядке времени
    private List<String> auditTrail(String eventId) {
        return jdbcClient.sql("""
                        SELECT concat(toString(outcome), '/', toString(attempt), '/', instance_id)
                        FROM device_publication_audit WHERE event_id = :id
                        ORDER BY occurred_at, attempt
                        """)
                .param("id", eventId)
                .query(String.class)
                .list();
    }
    
    private static List<DeviceInfo> publishedDeviceInfo(String deviceId) {
        deviceInfoKafkaConsumer.poll(Duration.ofMillis(500)).forEach(devicesSeen::add);
        return devicesSeen.stream()
                .filter(record -> deviceId.equals(record.key()))
                .map(ConsumerRecord::value)
                .toList();
    }

    // traceId из заголовка traceparent сообщений устройства в devices (его добавляет KafkaTemplate)
    private static List<String> publishedTraceIds(String deviceId) {
        publishedDeviceInfo(deviceId);
        return devicesSeen.stream()
                .filter(record -> deviceId.equals(record.key()))
                .map(record -> record.headers().lastHeader("traceparent"))
                .filter(Objects::nonNull)
                .map(header -> new String(header.value(), StandardCharsets.UTF_8).split("-")[1])
                .toList();
    }
}
