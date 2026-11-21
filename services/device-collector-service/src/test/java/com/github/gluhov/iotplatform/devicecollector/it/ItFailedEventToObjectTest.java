package com.github.gluhov.iotplatform.devicecollector.it;

import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import com.github.gluhov.iotplatform.events.avro.FailedEvent;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.github.gluhov.iotplatform.devicecollector.data.TestData.info;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Сообщение devices, которое сборщик не смог обработать, - до объекта в бакете failed-events (FR-004, FR-023):
 * сборщик публикует FailedEvent, процессор (второй контекст, R23) пишет объект по ключу
 * {@code device-collector-service/<дата>/<код>/devices-<раздел>-<смещение>.json}. Объект несёт исходные байты ключа
 * и значения, код, тип, число попыток, трассу входящего сообщения (traceparent) и заголовки исходной записи.
 */
class ItFailedEventToObjectTest extends AbstractItTest {

    private static final String SERVICE = "device-collector-service";
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyy/MM/dd").withZone(ZoneOffset.UTC);
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String TRACEPARENT = "00-" + TRACE_ID + "-00f067aa0ba902b7-01";
    // Время событий - в прошлом относительно часов сервиса (будущее время отклоняет валидатор)
    private static final Instant T = Instant.now().minus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.MILLIS);

    @Test
    void givenInvalidMessageWithTraceparent_whenFailed_thenObjectWithSourceBytesAndTrace() throws Exception {
        String id = deviceOnShard(0);
        byte[] source = avroBytes(DEVICES, info(id, "evt-obj-1", DeviceUpsertEventType.CREATED, T).build());
        // Ключ не совпадает с deviceId - валидация отклоняет сообщение
        byte[] key = ("another-key-" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);

        RecordMetadata sent = sendBytes(key, source, 0, traceparent());

        String archiveKey = archiveKey("VALIDATION_FAILED", sent);
        awaitObject(archiveKey);
        JsonNode doc = readJson(archiveKey);
        assertThat(doc.get("sourceService").asString()).isEqualTo(SERVICE);
        assertThat(doc.get("sourceTopic").asString()).isEqualTo(DEVICES);
        assertThat(doc.get("sourcePartition").asInt()).isEqualTo(sent.partition());
        assertThat(doc.get("sourceOffset").asLong()).isEqualTo(sent.offset());
        assertThat(doc.get("sourceTimestamp").asString()).isEqualTo(Instant.ofEpochMilli(sent.timestamp()).toString());
        assertThat(doc.get("errorCode").asString()).isEqualTo("VALIDATION_FAILED");
        assertThat(doc.get("errorType").asString()).isEqualTo("PERMANENT");
        assertThat(doc.get("attempts").asInt()).isEqualTo(1);
        assertThat(doc.get("description").asString()).contains("InvalidDeviceInfoException");
        assertThat(doc.get("deviceId").asString()).isEqualTo(id);
        assertThat(doc.get("traceId").asString()).as("трасса входящего сообщения").isEqualTo(TRACE_ID);
        assertThat(decodeField(doc, "key")).as("исходный ключ побайтно").isEqualTo(key);
        assertThat(decodeField(doc, "value")).as("исходное значение побайтно").isEqualTo(source);
        assertThat(decodeHeader(doc.get("headers"), "traceparent")).isEqualTo(TRACEPARENT.getBytes(StandardCharsets.US_ASCII));
        assertThat(s3.headObject(b -> b.bucket(BUCKET).key(archiveKey)).metadata())
                .containsEntry("error-code", "VALIDATION_FAILED")
                .containsEntry("source-service", SERVICE);
    }

    @Test
    void givenShardDown_whenAttemptsExhausted_thenObjectWithStorageErrorAndThreeAttempts() throws Exception {
        String id = deviceOnShard(1);
        byte[] source = avroBytes(DEVICES, info(id, "evt-obj-2", DeviceUpsertEventType.CREATED, T).build());
        String archiveKey;
        stopShard(SHARD_1);
        try {
            RecordMetadata sent = sendBytes(id.getBytes(StandardCharsets.UTF_8), source, 2, traceparent());
            archiveKey = archiveKey("STORAGE_UNAVAILABLE", sent);
            awaitObject(archiveKey);
        } finally {
            startShard(SHARD_1);
        }

        JsonNode doc = readJson(archiveKey);
        assertThat(doc.get("errorCode").asString()).isEqualTo("STORAGE_UNAVAILABLE");
        assertThat(doc.get("errorType").asString()).isEqualTo("TRANSIENT");
        assertThat(doc.get("attempts").asInt()).as("max-attempts попыток, затем объект").isEqualTo(3);
        assertThat(doc.get("deviceId").asString()).isEqualTo(id);
        assertThat(doc.get("traceId").asString()).isEqualTo(TRACE_ID);
        assertThat(decodeField(doc, "value")).isEqualTo(source);
    }

    @Test
    void givenUnreadableValue_whenFailed_thenObjectWithRawBytes() throws Exception {
        byte[] garbage = "not-an-avro-message".getBytes(StandardCharsets.UTF_8);
        byte[] key = ("raw-" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);

        RecordMetadata sent = sendBytes(key, garbage, 1, traceparent());

        String archiveKey = archiveKey("DESERIALIZATION_FAILED", sent);
        awaitObject(archiveKey);
        JsonNode doc = readJson(archiveKey);
        assertThat(doc.get("errorCode").asString()).isEqualTo("DESERIALIZATION_FAILED");
        assertThat(doc.get("errorType").asString()).isEqualTo("PERMANENT");
        assertThat(doc.get("attempts").asInt()).isEqualTo(1);
        assertThat(doc.get("deviceId").isNull()).as("разобрать не удалось - устройства нет").isTrue();
        assertThat(doc.get("traceId").asString()).isEqualTo(TRACE_ID);
        assertThat(decodeField(doc, "value")).as("исходные байты значения").isEqualTo(garbage);
        assertThat(decodeField(doc, "key")).isEqualTo(key);
    }

    @Test
    void givenKeyNotUtf8_whenFailed_thenObjectWithRawKeyBytes() throws Exception {
        // Ключ не строгий UTF-8 (FF FE 41), значение - корректный DeviceInfo
        byte[] badKey = {(byte) 0xFF, (byte) 0xFE, 0x41};
        byte[] source = avroBytes(DEVICES, info(deviceOnShard(0), "evt-obj-3", DeviceUpsertEventType.CREATED, T).build());

        RecordMetadata sent = sendBytes(badKey, source, 0, traceparent());

        String archiveKey = archiveKey("DESERIALIZATION_FAILED", sent);
        awaitObject(archiveKey);
        JsonNode doc = readJson(archiveKey);
        assertThat(doc.get("errorCode").asString()).isEqualTo("DESERIALIZATION_FAILED");
        assertThat(decodeField(doc, "key")).as("ключ - исходные байты, не U+FFFD").isEqualTo(badKey);
        assertThat(decodeField(doc, "value")).isEqualTo(source);
    }

    @Test
    void givenSameFailedEventPublishedTwice_whenProcessed_thenOneObjectOneVersion() throws Exception {
        byte[] key = ("dup-" + UUID.randomUUID()).getBytes(StandardCharsets.UTF_8);
        byte[] source = avroBytes(DEVICES, info(deviceOnShard(0), "evt-obj-4", DeviceUpsertEventType.CREATED, T).build());
        RecordMetadata original = sendBytes(key, source, 0, traceparent());
        String archiveKey = archiveKey("VALIDATION_FAILED", original);
        awaitObject(archiveKey);
        FailedEvent event = failedEvent(original).orElseThrow();

        // Сборщик повторно опубликовал то же FailedEvent: те же исходные координаты, тот же ключ записи
        RecordMetadata again = publishFailedEvent(original, event);

        awaitProcessed(again);
        assertThat(versionCount(archiveKey)).as("повтор публикации не даёт второго объекта и второй версии (FR-008)").isEqualTo(1);
    }

    // ------------------------------------------------------------------ Kafka

    private static List<Header> traceparent() {
        return List.of(new RecordHeader("traceparent", TRACEPARENT.getBytes(StandardCharsets.US_ASCII)));
    }

    /** Сырые байты ключа и значения, как их пишет внешний продюсер (без сериализатора сборщика). */
    private static RecordMetadata sendBytes(byte[] key, byte[] value, int partition, List<Header> headers) throws Exception {
        try (var producer = new KafkaProducer<byte[], byte[]>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            return producer.send(new ProducerRecord<>(DEVICES, partition, key, value, headers)).get();
        }
    }

    /** Повторная публикация FailedEvent в failed-events с ключом исходной записи (как у сборщика). */
    private static RecordMetadata publishFailedEvent(RecordMetadata original, FailedEvent event) throws Exception {
        String sourceKey = original.topic() + "-" + original.partition() + "-" + original.offset();
        try (var producer = new KafkaProducer<String, Object>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class,
                "schema.registry.url", schemaRegistryUrl()))) {
            return producer.send(new ProducerRecord<>(FAILED, sourceKey, event)).get();
        }
    }

    /** Процессор подтвердил запись: смещение его группы больше смещения записи. */
    private static void awaitProcessed(RecordMetadata record) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        await().atMost(WAIT).untilAsserted(() -> {
            try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
                var committed = admin.listConsumerGroupOffsets(PROCESSOR_GROUP).partitionsToOffsetAndMetadata().get()
                        .get(partition);
                assertThat(committed).isNotNull();
                assertThat(committed.offset()).isGreaterThan(record.offset());
            }
        });
    }

    // ------------------------------------------------------------------ MinIO

    /** Ключ объекта: дата - время исходного сообщения (UTC), как ArchiveKey процессора. */
    private static String archiveKey(String errorCode, RecordMetadata source) {
        return SERVICE + "/" + DAY.format(Instant.ofEpochMilli(source.timestamp())) + "/" + errorCode + "/"
                + DEVICES + "-" + source.partition() + "-" + source.offset() + ".json";
    }

    private static void awaitObject(String key) {
        try {
            await().atMost(WAIT).pollInterval(Duration.ofMillis(500)).until(() -> exists(key));
        } catch (ConditionTimeoutException e) {
            throw new AssertionError("Объект " + key + " не записан за " + WAIT + ". В бакете: "
                    + s3.listObjectsV2(b -> b.bucket(BUCKET)).contents().stream()
                    .map(S3Object::key).toList() + ". " + processorState(), e);
        }
    }

    private static boolean exists(String key) {
        try {
            s3.headObject(b -> b.bucket(BUCKET).key(key));
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    private static JsonNode readJson(String key) {
        return JSON.readTree(s3.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray());
    }

    private static int versionCount(String key) {
        return (int) s3.listObjectVersions(b -> b.bucket(BUCKET).prefix(key)).versions().stream()
                .filter(version -> version.key().equals(key))
                .count();
    }

    /** Диагностика падения: разделы failed-events, их концы и смещения группы процессора. */
    private static String processorState() {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            List<TopicPartition> partitions = admin.describeTopics(List.of(FAILED)).allTopicNames().get().get(FAILED)
                    .partitions().stream().map(p -> new TopicPartition(FAILED, p.partition())).toList();
            Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
            partitions.forEach(p -> latest.put(p, OffsetSpec.latest()));
            var end = admin.listOffsets(latest).all().get();
            var committed = admin.listConsumerGroupOffsets(PROCESSOR_GROUP).partitionsToOffsetAndMetadata().get();
            return "failed-events: разделов " + partitions.size() + ", концы " + end.entrySet().stream()
                    .map(e -> e.getKey().partition() + ":" + e.getValue().offset()).toList()
                    + ", смещения процессора " + committed.entrySet().stream()
                    .map(e -> e.getKey().partition() + ":" + e.getValue().offset()).toList();
        } catch (Exception e) {
            return "состояние темы недоступно: " + e;
        }
    }

    /** Байты base64-поля JSON-документа. */
    private static byte[] decodeField(JsonNode document, String field) {
        return Base64.getDecoder().decode(document.get(field).asString());
    }

    /** Байты заголовка исходной записи (объект headers, значения - base64). */
    private static byte[] decodeHeader(JsonNode headers, String name) {
        return Base64.getDecoder().decode(headers.get(name).asString());
    }
}
