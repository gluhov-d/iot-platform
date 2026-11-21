package com.github.gluhov.iotplatform.eventscollector.it;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.events.avro.ErrorCode;
import com.github.gluhov.iotplatform.events.avro.ErrorType;
import com.github.gluhov.iotplatform.events.avro.FailedEvent;
import com.github.gluhov.iotplatform.eventscollector.config.SchemaRegistryTestContainerConfig;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.DeviceEventRepository;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.github.gluhov.iotplatform.eventscollector.data.TestData.unique;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;

/**
 * «До объекта в MinIO» (FR-004, FR-023): сборщик публикует FailedEvent в failed-events, процессор - вторым
 * Spring-контекстом этой JVM - пишет объект в бакет failed-events (Object Lock и версии, как на стенде).
 * Проверяются сценарии: невалидное сообщение, исчерпаны попытки, нечитаемые байты, повторная публикация.
 */
@TestPropertySource(properties = {
        "app.kafka.events-topic=" + ItFailedEventToObjectTest.TOPIC,
        "app.processing.max-attempts=" + ItFailedEventToObjectTest.MAX_ATTEMPTS,
        "app.processing.retry-interval=100ms",
        "spring.kafka.consumer.group-id=" + ItFailedEventToObjectTest.GROUP
})
class ItFailedEventToObjectTest extends AbstractItTest {

    static final String TOPIC = "events-object-it";
    static final String GROUP = "events-collector-object-it";
    static final int MAX_ATTEMPTS = 3;

    private static final String FAILED = "failed-events";
    private static final String BUCKET = "failed-events";
    private static final String PROCESSOR_GROUP = "failed-events-processor-object-it";
    private static final String SOURCE_SERVICE = "events-collector-service";
    private static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String PARENT_SPAN_ID = "00f067aa0ba902b7";
    private static final Duration WAIT = Duration.ofSeconds(60);
    private static final JsonMapper JSON = JsonMapper.builder().build();
    /** Смещения исходных сообщений уникальны на JVM: ключи объектов не пересекаются между тестами. */
    private static final AtomicLong NEXT_OFFSET = new AtomicLong(System.currentTimeMillis());

    /** Образ MinIO из реестра стенда (как процессор). */
    private static final MinIOContainer MINIO = new MinIOContainer(
            DockerImageName.parse("pgsty/minio:RELEASE.2026-08-04T00-00-00Z").asCompatibleSubstituteFor("minio/minio"));

    private static S3Client s3;
    private static ConfigurableApplicationContext processor;

    static {
        Startables.deepStart(MINIO).join();
        s3 = S3Client.builder()
                .endpointOverride(URI.create(MINIO.getS3URL()))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(MINIO.getUserName(), MINIO.getPassword())))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
        // Бакет как на стенде: Object Lock включает версии
        s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).objectLockEnabledForBucket(true).build());
        createTopic();
    }

    @MockitoSpyBean
    private DeviceEventRepository eventRepository;

    @BeforeAll
    static void startProcessor() {
        processor = FailedEventsProcessorContext.start(
                kafka.getBootstrapServers(), SchemaRegistryTestContainerConfig.schemaRegistryUrl(),
                FAILED, PROCESSOR_GROUP, MINIO.getS3URL(), BUCKET, MINIO.getUserName(), MINIO.getPassword());
    }

    @AfterAll
    static void stopProcessor() {
        if (processor != null) {
            processor.close();
        }
    }

    @Test
    void givenInvalidEvent_whenIngested_thenObjectWithSourceBytesAndTraceFromTraceparent() throws Exception {
        byte[] raw = avroBytes(deviceEvent("   "));

        RecordMetadata source = sendRaw(raw, List.of(traceparent()));

        JsonNode doc = awaitObject(source.offset(), "VALIDATION_FAILED");
        assertThat(doc.get("errorType").asText()).isEqualTo(ErrorType.PERMANENT.name());
        assertThat(doc.get("attempts").asInt()).isEqualTo(1);
        assertThat(doc.get("sourceService").asText()).isEqualTo(SOURCE_SERVICE);
        assertThat(doc.get("sourceTopic").asText()).isEqualTo(TOPIC);
        assertThat(doc.get("sourceOffset").asLong()).isEqualTo(source.offset());
        assertThat(doc.get("traceId").asText()).isEqualTo(TRACE_ID);
        assertThat(bytes(doc, "value")).as("исходные байты").isEqualTo(raw);
    }

    @Test
    void givenStorageFailureAfterMaxAttempts_whenIngested_thenObjectWithStorageErrorAndAttempts() throws Exception {
        String failing = unique("evt");
        doThrow(new IllegalStateException("clickhouse is down"))
                .when(eventRepository).insert(argThat(record -> record != null && failing.equals(record.eventId())));
        byte[] raw = avroBytes(deviceEvent(failing));

        RecordMetadata source = sendRaw(raw, List.of(traceparent()));

        JsonNode doc = awaitObject(source.offset(), "STORAGE_UNAVAILABLE");
        assertThat(doc.get("errorType").asText()).isEqualTo(ErrorType.TRANSIENT.name());
        assertThat(doc.get("attempts").asInt()).isEqualTo(MAX_ATTEMPTS);
        assertThat(doc.get("description").asText()).contains("IllegalStateException: clickhouse is down");
        assertThat(doc.get("traceId").asText()).isEqualTo(TRACE_ID);
        assertThat(bytes(doc, "value")).as("исходные байты").isEqualTo(raw);
    }

    @Test
    void givenUnreadableBytes_whenIngested_thenObjectWithOriginalKeyAndValue() throws Exception {
        byte[] garbage = "not-an-avro-message".getBytes(StandardCharsets.UTF_8);

        RecordMetadata source = sendRaw(garbage, List.of(traceparent()));

        JsonNode doc = awaitObject(source.offset(), "DESERIALIZATION_FAILED");
        assertThat(doc.get("errorType").asText()).isEqualTo(ErrorType.PERMANENT.name());
        assertThat(doc.get("attempts").asInt()).isEqualTo(1);
        assertThat(doc.get("sourceService").asText()).isEqualTo(SOURCE_SERVICE);
        assertThat(doc.get("traceId").asText()).isEqualTo(TRACE_ID);
        assertThat(bytes(doc, "value")).as("исходные байты").isEqualTo(garbage);
        assertThat(bytes(doc, "key")).as("исходный ключ").isEqualTo("dev-errors".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void givenSameFailedEventPublishedTwice_whenProcessed_thenSingleObjectVersion() throws Exception {
        long offset = nextOffset();
        FailedEvent event = failedEvent(offset);

        // Повторная публикация одного исходного сообщения (сборщик упал после отправки и отправил снова)
        sendFailedEvent(event);
        RecordMetadata second = sendFailedEvent(event);

        String key = objectKey(offset, "VALIDATION_FAILED");
        // Процессор прошёл оба сообщения: второе подтверждено (412 - дубликат, смещение подтверждается)
        await().atMost(WAIT).untilAsserted(() -> assertThat(committedOffset()).isGreaterThan(second.offset()));
        assertThat(versionCount(key)).as("второй объект не создан (условная запись дала 412)").isEqualTo(1);
    }

    // ------------------------------------------------------------------ сообщения сборщика

    private static DeviceEvent deviceEvent(String eventId) {
        return DeviceEvent.newBuilder()
                .setEventId(eventId)
                .setDeviceId("dev-errors")
                .setTimestampMs(Instant.now().toEpochMilli())
                .setEventType("TELEMETRY")
                .build();
    }

    /** Байты записи как их пишет продюсер с KafkaAvroSerializer (схема в Schema Registry уже есть). */
    private static byte[] avroBytes(DeviceEvent event) {
        try (var serializer = new KafkaAvroSerializer()) {
            serializer.configure(Map.of("schema.registry.url", SchemaRegistryTestContainerConfig.schemaRegistryUrl()), false);
            return serializer.serialize(TOPIC, event);
        }
    }

    private static Header traceparent() {
        return new RecordHeader("traceparent", ("00-" + TRACE_ID + "-" + PARENT_SPAN_ID + "-01")
                .getBytes(StandardCharsets.UTF_8));
    }

    private static RecordMetadata sendRaw(byte[] value, List<Header> headers) throws Exception {
        try (var producer = new KafkaProducer<String, byte[]>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            return producer.send(new ProducerRecord<>(TOPIC, 0, "dev-errors", value, headers)).get();
        }
    }

    /** Запись failed-events, как пишет сборщик (ключ записи - исходные координаты). */
    private static RecordMetadata sendFailedEvent(FailedEvent event) throws Exception {
        String key = event.getSourceTopic() + "-" + event.getSourcePartition() + "-" + event.getSourceOffset();
        try (var producer = new KafkaProducer<String, Object>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class,
                "schema.registry.url", SchemaRegistryTestContainerConfig.schemaRegistryUrl()))) {
            return producer.send(new ProducerRecord<>(FAILED, 0, key, event)).get();
        }
    }

    private static FailedEvent failedEvent(long sourceOffset) {
        return FailedEvent.newBuilder()
                .setSourceService(SOURCE_SERVICE)
                .setSourceTopic(TOPIC)
                .setSourcePartition(0)
                .setSourceOffset(sourceOffset)
                .setSourceTimestamp(Instant.now())
                .setFailedAt(Instant.now())
                .setErrorCode(ErrorCode.VALIDATION_FAILED)
                .setErrorType(ErrorType.PERMANENT)
                .setDescription("InvalidEventException: eventId must not be blank")
                .setTraceId(TRACE_ID)
                .setKey(ByteBuffer.wrap("dev-errors".getBytes(StandardCharsets.UTF_8)))
                .setValue(ByteBuffer.wrap(new byte[]{0, 0, 0, 0, 1, 12, 'd', 'e', 'v'}))
                .setHeaders(Map.of())
                .setAttempts(1)
                .setDeviceId("dev-errors")
                .build();
    }

    // ------------------------------------------------------------------ MinIO

    /** Объект ошибки для исходного смещения: ключ - {@code <сервис>/<дата>/<код>/<тема>-<раздел>-<смещение>.json}. */
    private static JsonNode awaitObject(long sourceOffset, String errorCode) {
        String key = await().atMost(WAIT).pollInterval(Duration.ofMillis(500))
                .until(() -> findKey(sourceOffset, errorCode), java.util.Optional::isPresent).orElseThrow();
        return JSON.readTree(s3.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray());
    }

    private static String objectKey(long sourceOffset, String errorCode) {
        return findKey(sourceOffset, errorCode).orElseThrow();
    }

    private static java.util.Optional<String> findKey(long sourceOffset, String errorCode) {
        String suffix = "/" + errorCode + "/" + TOPIC + "-0-" + sourceOffset + ".json";
        return s3.listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(BUCKET).prefix(SOURCE_SERVICE + "/")
                        .build())
                .contents().stream().map(S3Object::key)
                .filter(key -> key.endsWith(suffix))
                .findFirst();
    }

    private static int versionCount(String key) {
        return (int) s3.listObjectVersionsPaginator(ListObjectVersionsRequest.builder().bucket(BUCKET).prefix(key)
                        .build())
                .versions().stream().filter(version -> version.key().equals(key)).count();
    }

    private static byte[] bytes(JsonNode doc, String field) {
        return Base64.getDecoder().decode(doc.get(field).asText());
    }

    private static long nextOffset() {
        return NEXT_OFFSET.incrementAndGet();
    }

    // ------------------------------------------------------------------ Kafka

    private static void createTopic() {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            try {
                admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw e;
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Не создана тема " + TOPIC, e);
        }
    }

    /** Подтверждённое смещение группы процессора в failed-events (-1 - ещё ничего не подтверждено). */
    private static long committedOffset() throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            OffsetAndMetadata committed = admin.listConsumerGroupOffsets(PROCESSOR_GROUP)
                    .partitionsToOffsetAndMetadata().get().get(new TopicPartition(FAILED, 0));
            return committed == null ? -1 : committed.offset();
        }
    }
}
