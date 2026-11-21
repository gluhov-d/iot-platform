package com.github.gluhov.iotplatform.failedeventsprocessor.it;

import com.github.gluhov.iotplatform.events.avro.ErrorCode;
import com.github.gluhov.iotplatform.events.avro.ErrorType;
import com.github.gluhov.iotplatform.events.avro.FailedEvent;
import com.github.gluhov.iotplatform.failedeventsprocessor.FailedEventsProcessorApplication;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
import software.amazon.awssdk.services.s3.model.S3Exception;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Интеграционные тесты процессора: Kafka, Schema Registry и MinIO (бакет с Object Lock и версиями) в Testcontainers,
 * процессор - полный Spring-контекст с конфигурацией failed-events-processor.yaml. Контейнеры поднимаются один раз
 * на JVM и общие для всех классов It*Test (контекст тоже один: одинаковая конфигурация).
 */
@SpringBootTest
@Import(SpanCaptureConfig.class)
public abstract class AbstractItTest {

    protected static final String FAILED = "failed-events";
    protected static final String BUCKET = "failed-events";
    protected static final String GROUP = "failed-events-processor-it";
    protected static final Duration WAIT = Duration.ofSeconds(60);
    protected static final JsonMapper JSON = JsonMapper.builder().build();
    protected static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    protected static final String PARENT_SPAN_ID = "00f067aa0ba902b7";
    protected static final String SOURCE_KEY_PREFIX = "events-collector-service/2026/11/21/VALIDATION_FAILED/events-2-";

    private static final Network NETWORK = Network.newNetwork();
    private static final String KAFKA_INTERNAL = "kafka:19092";
    /** Смещения исходных сообщений уникальны на JVM: ключи объектов не пересекаются между тестами. */
    private static final AtomicLong NEXT_SOURCE_OFFSET = new AtomicLong(System.currentTimeMillis());

    protected static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.0")
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka")
            .withListener(KAFKA_INTERNAL)
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
    protected static final GenericContainer<?> SCHEMA_REGISTRY = new GenericContainer<>("confluentinc/cp-schema-registry:8.2.4")
            .withNetwork(NETWORK)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + KAFKA_INTERNAL)
            .dependsOn(KAFKA)
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(2)));
    /** Образ MinIO из реестра стенда, подставлен вместо minio/minio (модуль Testcontainers проверяет имя образа). */
    protected static final MinIOContainer MINIO = new MinIOContainer(
            DockerImageName.parse("pgsty/minio:RELEASE.2026-08-04T00-00-00Z").asCompatibleSubstituteFor("minio/minio"));

    protected static S3Client s3;

    static {
        // Имя файла конфигурации - как у запуска приложения (R23): application.yaml в сервисе нет
        FailedEventsProcessorApplication.configureConfigName();
        Startables.deepStart(KAFKA, SCHEMA_REGISTRY, MINIO).join();
        createTopic();
        registerFailedEventsSchema();
        s3 = S3Client.builder()
                .endpointOverride(URI.create(MINIO.getS3URL()))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(MINIO.getUserName(), MINIO.getPassword())))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
        // Бакет как на стенде: Object Lock включает версии
        s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).objectLockEnabledForBucket(true).build());
    }

    @Autowired
    protected InMemorySpanExporter spans;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("KAFKA_BOOTSTRAP_SERVERS", KAFKA::getBootstrapServers);
        registry.add("SCHEMA_REGISTRY_URL", AbstractItTest::schemaRegistryUrl);
        registry.add("KAFKA_FAILED_EVENTS_TOPIC", () -> FAILED);
        registry.add("FAILED_EVENTS_CONSUMER_GROUP", () -> GROUP);
        registry.add("FAILED_EVENTS_MAX_POLL_INTERVAL_MS", () -> "30000");
        registry.add("FAILED_EVENTS_RETRY_INITIAL_INTERVAL", () -> "500ms");
        registry.add("FAILED_EVENTS_RETRY_MAX_INTERVAL", () -> "10s");
        registry.add("FAILED_EVENTS_RETRY_MULTIPLIER", () -> "2");
        registry.add("FAILED_EVENTS_S3_ENDPOINT", MINIO::getS3URL);
        registry.add("FAILED_EVENTS_S3_REGION", () -> "us-east-1");
        registry.add("FAILED_EVENTS_S3_ACCESS_KEY", MINIO::getUserName);
        registry.add("FAILED_EVENTS_S3_SECRET_KEY", MINIO::getPassword);
        registry.add("FAILED_EVENTS_S3_CONNECT_TIMEOUT", () -> "2s");
        registry.add("FAILED_EVENTS_S3_API_CALL_TIMEOUT", () -> "8s");
        registry.add("FAILED_EVENTS_S3_API_CALL_ATTEMPT_TIMEOUT", () -> "4s");
        registry.add("FAILED_EVENTS_BUCKET", () -> BUCKET);
        // Экспорт трасс идёт в спаны в памяти (бин InMemorySpanExporter); адрес OTLP нужен только для свойства
        registry.add("MANAGEMENT_OTLP_ENDPOINT", () -> "http://localhost:4318/v1/traces");
    }

    protected static String schemaRegistryUrl() {
        return "http://%s:%d".formatted(SCHEMA_REGISTRY.getHost(), SCHEMA_REGISTRY.getMappedPort(8081));
    }

    private static void createTopic() {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(FAILED, 1, (short) 1))).all().get();
        } catch (Exception e) {
            throw new IllegalStateException("Не создана тема " + FAILED, e);
        }
    }

    /** Как задача schema-register на стенде: схема FailedEvent лежит в Schema Registry до запуска (auto.register=false). */
    private static void registerFailedEventsSchema() {
        try (var client = new CachedSchemaRegistryClient(schemaRegistryUrl(), 10)) {
            client.register(FAILED + "-value", new AvroSchema(FailedEvent.getClassSchema()));
        } catch (Exception e) {
            throw new IllegalStateException("Не зарегистрирована схема " + FAILED + "-value", e);
        }
    }

    // ------------------------------------------------------------------ Kafka

    /** Запись FailedEvent через Schema Registry, как пишет сборщик. */
    protected static RecordMetadata send(String key, FailedEvent value) throws Exception {
        return send(key, value, List.of());
    }

    protected static RecordMetadata send(String key, FailedEvent value, List<Header> headers) throws Exception {
        try (var producer = new KafkaProducer<String, Object>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class,
                "schema.registry.url", schemaRegistryUrl()))) {
            return producer.send(new ProducerRecord<>(FAILED, 0, key, value, headers)).get();
        }
    }

    /** Байты как есть (не Avro): такие записи должны уйти в unreadable. */
    protected static RecordMetadata sendRaw(String key, byte[] value) throws Exception {
        try (var producer = new KafkaProducer<String, byte[]>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            return producer.send(new ProducerRecord<>(FAILED, 0, key, value)).get();
        }
    }

    /** Ключ и значение как есть (байты): ключ может не быть валидным UTF-8. */
    protected static RecordMetadata sendRawWithKey(byte[] key, byte[] value) throws Exception {
        try (var producer = new KafkaProducer<byte[], byte[]>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            return producer.send(new ProducerRecord<>(FAILED, 0, key, value)).get();
        }
    }

    /** Смещение группы больше смещения сообщения: процессор подтвердил запись (объект или unreadable). */
    protected static void awaitCommitted(RecordMetadata message) {
        await().atMost(WAIT).untilAsserted(() ->
                assertThat(committedOffset()).isGreaterThan(message.offset()));
    }

    /** Подтверждённое смещение группы (следующее для чтения); -1 - группа ещё ничего не подтвердила. */
    protected static long committedOffset() throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            OffsetAndMetadata committed = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get()
                    .get(new TopicPartition(FAILED, 0));
            return committed == null ? -1 : committed.offset();
        }
    }

    protected static String groupState() throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            return admin.describeConsumerGroups(List.of(GROUP)).describedGroups().get(GROUP).get().state().toString();
        }
    }

    protected static Header header(String name, byte[] value) {
        return new RecordHeader(name, value);
    }

    // ------------------------------------------------------------------ FailedEvent

    /** Исходное сообщение events-collector-service с уникальным смещением (ключ объекта - по нему). */
    protected static FailedEvent event(long sourceOffset, String traceId, Instant failedAt) {
        return FailedEvent.newBuilder()
                .setSourceService("events-collector-service")
                .setSourceTopic("events")
                .setSourcePartition(2)
                .setSourceOffset(sourceOffset)
                .setSourceTimestamp(Instant.parse("2026-11-21T10:15:30.123Z"))
                .setFailedAt(failedAt)
                .setErrorCode(ErrorCode.VALIDATION_FAILED)
                .setErrorType(ErrorType.PERMANENT)
                .setDescription("InvalidEventException: value must be finite")
                .setTraceId(traceId)
                .setKey(ByteBuffer.wrap("dev-42".getBytes(StandardCharsets.UTF_8)))
                .setValue(ByteBuffer.wrap(new byte[]{0, 0, 0, 0, 1, 12, 'd', 'e', 'v', (byte) 0xFF}))
                .setHeaders(Map.of())
                .setAttempts(1)
                .setDeviceId("dev-42")
                .build();
    }

    protected static long nextSourceOffset() {
        return NEXT_SOURCE_OFFSET.incrementAndGet();
    }

    protected static String sourceKey(long sourceOffset) {
        return SOURCE_KEY_PREFIX + sourceOffset + ".json";
    }

    /** Ключ объекта unreadable: дата - время записи в failed-events (UTC). */
    protected static String unreadableKey(RecordMetadata record) {
        return "unreadable/" + java.time.format.DateTimeFormatter.ofPattern("yyyy/MM/dd")
                .withZone(java.time.ZoneOffset.UTC).format(Instant.ofEpochMilli(record.timestamp()))
                + "/failed-events-" + record.partition() + "-" + record.offset() + ".json";
    }

    // ------------------------------------------------------------------ MinIO

    protected static boolean exists(String key) {
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

    protected static HeadObjectResponse head(String key) {
        return s3.headObject(b -> b.bucket(BUCKET).key(key));
    }

    protected static void awaitObject(String key) {
        await().atMost(WAIT).pollInterval(Duration.ofMillis(500)).until(() -> exists(key));
    }

    protected static JsonNode readJson(String key) {
        return JSON.readTree(s3.getObjectAsBytes(b -> b.bucket(BUCKET).key(key)).asByteArray());
    }

    protected static int versionCount(String key) {
        List<ObjectVersion> versions = s3.listObjectVersions(b -> b.bucket(BUCKET).prefix(key)).versions();
        List<ObjectVersion> same = new ArrayList<>();
        for (ObjectVersion version : versions) {
            if (version.key().equals(key)) {
                same.add(version);
            }
        }
        return same.size();
    }

    /** Заморозить контейнер (docker pause): адрес и порт сохраняются. */
    protected static void pause(GenericContainer<?> container) {
        DockerClientFactory.instance().client().pauseContainerCmd(container.getContainerId()).exec();
    }

    protected static void unpause(GenericContainer<?> container) {
        String id = container.getContainerId();
        if (Boolean.TRUE.equals(DockerClientFactory.instance().client().inspectContainerCmd(id).exec()
                .getState().getPaused())) {
            DockerClientFactory.instance().client().unpauseContainerCmd(id).exec();
        }
    }

}
