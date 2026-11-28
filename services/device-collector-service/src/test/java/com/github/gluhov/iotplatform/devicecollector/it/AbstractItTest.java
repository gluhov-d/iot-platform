package com.github.gluhov.iotplatform.devicecollector.it;

import com.github.gluhov.iotplatform.dbmigrator.DbMigrator;
import com.github.gluhov.iotplatform.dbmigrator.ShardTarget;
import com.github.gluhov.iotplatform.events.avro.FailedEvent;
import com.github.gluhov.iotplatform.failedeventsprocessor.FailedEventsProcessorApplication;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ActiveProfiles;
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
import org.testcontainers.utility.MountableFile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Стенд интеграционных тестов - те же компоненты и та же конфигурация, что в docker-compose.yaml:
 * Kafka (темы - init-topics.sh), Schema Registry, два шарда PostgreSQL 18, схема - db-migrator,
 * ShardingSphere-Proxy 5.5.3 с конфигурацией infrastructure/config/shardingsphere и её точкой входа render-config.sh.
 * Контейнеры поднимаются один раз на JVM. Сервис знает только адрес Proxy.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"test"})
public abstract class AbstractItTest {
    protected static final Path REPO_ROOT = Path.of("../..").toAbsolutePath().normalize();
    protected static final String DEVICES = "devices";
    protected static final String FAILED = "failed-events";
    protected static final String GROUP = "device-collector-it";
    protected static final Duration WAIT = Duration.ofSeconds(60);
    protected static final String BUCKET = "failed-events";
    /** Группа процессора во втором контексте (R23): своя, не совпадает с группой сборщика. */
    protected static final String PROCESSOR_GROUP = "failed-events-processor-dcs-it";

    private static final Network NETWORK = Network.newNetwork();
    private static final String KAFKA_INTERNAL = "kafka:19092";
    private static final String SHARD_USER = "devices";
    // Пароли с кавычками, #, \, & и пробелом: render-config.sh должен подставить их в YAML Proxy без искажений
    private static final String SHARD_PASSWORD="it-shard p'a\"s#s\\&1";
    protected static final String PROXY_USER = "devices_app";
    protected static final String PROXY_PASSWORD="it-proxy p'a\"s#s\\&2";
    // Учётная запись чтения через Proxy (база devices_read) и роль device_reader на репликах
    private static final String READ_PROXY_USER = "devices_read_app";
    private static final String READ_PROXY_PASSWORD = "it-read-proxy-pw";
    private static final String READER_PASSWORD = "it-reader-pw";

    protected static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.2.0")
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka")
            .withListener(KAFKA_INTERNAL)
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
            .withCopyFileToContainer(MountableFile.forHostPath(
                    REPO_ROOT.resolve("infrastructure/init/kafka/init-topics.sh"), 0755), "/opt/init/init-topics.sh");
    protected static final GenericContainer<?> SCHEMA_REGISTRY = new GenericContainer<>("confluentinc/cp-schema-registry:8.2.4")
            .withNetwork(NETWORK)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + KAFKA_INTERNAL)
            .dependsOn(KAFKA)
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200).withStartupTimeout(Duration.ofMinutes(2)));
    protected static final GenericContainer<?> SHARD_0 = shard(0);
    protected static final GenericContainer<?> SHARD_1 = shard(1);
    // Реплики шардов: Proxy загружает логическую базу devices_read только при наличии обеих реплик (ADR-0004).
    // Здесь - отдельные базы со схемой и ролью device_reader без репликации: DCS на реплики не ходит
    protected static final GenericContainer<?> REPLICA_0 = replica(0);
    protected static final GenericContainer<?> REPLICA_1 = replica(1);
    // Хранилище архива ошибок: образ MinIO из реестра стенда (как в процессоре; модуль Testcontainers проверяет имя образа)
    protected static final MinIOContainer MINIO = new MinIOContainer(
            DockerImageName.parse("pgsty/minio:RELEASE.2026-08-04T00-00-00Z").asCompatibleSubstituteFor("minio/minio"));
    protected static S3Client s3;
    protected static ConfigurableApplicationContext failedEventsProcessor;
    // Redis общий для платформы: кэш устройства (device:cache:v1:{deviceId}) - сброс после APPLIED. На стенде - через Sentinel
    // Недоступность Redis и Proxy в тестах - пауза контейнера (docker pause): порт и адрес не меняются, пулы
    // соединений продолжают работать после снятия паузы. Остановка и запуск дали бы другой порт на хосте
    protected static final GenericContainer<?> REDIS = new GenericContainer<>("redis:8.6.7-alpine")
            .withNetwork(NETWORK)
            .withNetworkAliases("redis")
            .withExposedPorts(6379)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(1)));
    protected static final GenericContainer<?> PROXY = new GenericContainer<>("apache/shardingsphere-proxy:5.5.3")
            .withNetwork(NETWORK)
            .withNetworkAliases("shardingsphere-proxy")
            .withExposedPorts(3307)
            .withEnv("PORT", "3307")
            .withEnv("CGROUP_MEM_OPTS", "-Xms256m -Xmx512m")
            .withEnv("SHARD_DB_USER", SHARD_USER)
            .withEnv("SHARD_DB_PASSWORD", SHARD_PASSWORD)
            .withEnv("SHARD_STATEMENT_TIMEOUT_MS", "5000")
            .withEnv("SHARD_LOCK_TIMEOUT_MS", "3000")
            .withEnv("SHARD_CONNECTION_TIMEOUT_MS", "3000")
            .withEnv("SHARDING_PROXY_USER", PROXY_USER)
            .withEnv("SHARDING_PROXY_PASSWORD", PROXY_PASSWORD)
            .withEnv("DEVICES_READ_DB_USER", READ_PROXY_USER)
            .withEnv("DEVICES_READ_DB_PASSWORD", READ_PROXY_PASSWORD)
            .withEnv("SHARD_READER_PASSWORD", READER_PASSWORD)
            .withCopyFileToContainer(MountableFile.forHostPath(REPO_ROOT.resolve("infrastructure/config/shardingsphere")),
                    "/opt/init/conf")
            .withCopyFileToContainer(MountableFile.forHostPath(
                    REPO_ROOT.resolve("infrastructure/config/shardingsphere/render-config.sh"), 0755), "/opt/init/render-config.sh")
            .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("/bin/bash", "/opt/init/render-config.sh"))
            .waitingFor(Wait.forLogMessage(".*ShardingSphere-Proxy .* mode started successfully.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(3)));

    static {
        Startables.deepStart(KAFKA, SCHEMA_REGISTRY, SHARD_0, SHARD_1, REPLICA_0, REPLICA_1, REDIS, MINIO).join();
        createTopics();
        registerFailedEventsSchema();
        s3 = S3Client.builder()
                .endpointOverride(URI.create(MINIO.getS3URL()))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(MINIO.getUserName(), MINIO.getPassword())))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
        // Бакет как на стенде: Object Lock включает версии (FR-008)
        s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).objectLockEnabledForBucket(true).build());
        // Как на стенде: схема на шардах до старта Proxy - Proxy читает метаданные таблиц при старте
        DbMigrator.migrateAll(List.of(target(0, SHARD_0), target(1, SHARD_1)));
        // Роль чтения создаётся до миграции: V3 выдаёт ей права на devices (как на репликах стенда)
        createReaderRole(REPLICA_0, 0);
        createReaderRole(REPLICA_1, 1);
        DbMigrator.migrateAll(List.of(target(0, REPLICA_0), target(1, REPLICA_1)));
        PROXY.start();
        // Процессор failed-events - вторым контекстом в этой JVM (R23): записывает объекты в бакет failed-events
        failedEventsProcessor = new SpringApplicationBuilder(FailedEventsProcessorApplication.class)
                .web(WebApplicationType.NONE)
                .properties(failedEventsProcessorProperties())
                .run();
    }

    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Value("${local.server.port}")
    protected int port;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("KAFKA_BOOTSTRAP_SERVERS", KAFKA::getBootstrapServers);
        registry.add("SCHEMA_REGISTRY_URL", AbstractItTest::schemaRegistryUrl);
        // auto.register.schemas остаётся false, как на стенде: схему failed-events-value регистрирует registerFailedEventsSchema()
        // (на стенде - задача schema-register), а не продюсер сервиса
        registry.add("DEVICES_DB_URL", () -> proxyUrl());
        registry.add("DEVICES_DB_USERNAME", () -> PROXY_USER);
        registry.add("DEVICES_DB_PASSWORD", () -> PROXY_PASSWORD);
        registry.add("DEVICE_COLLECTOR_CONSUMER_GROUP", () -> GROUP);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    /**
     * Свойства процессора для второго контекста (R23): переменные из failed-events-processor.yaml по их именам.
     * spring.config.name - как у процессора, но свойством контекста (setDefaultProperties), а не System.setProperty:
     * глобальное свойство JVM повлияло бы на контекст сборщика.
     */
    protected static Map<String, Object> failedEventsProcessorProperties() {
        return Map.ofEntries(
                Map.entry("spring.config.name", "failed-events-processor"),
                // На classpath сборщика есть JDBC: у процессора своей базы нет, автоконфигурация DataSource упала бы
                Map.entry("spring.autoconfigure.exclude", "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration"),
                Map.entry("KAFKA_BOOTSTRAP_SERVERS", KAFKA.getBootstrapServers()),
                Map.entry("SCHEMA_REGISTRY_URL", schemaRegistryUrl()),
                Map.entry("KAFKA_FAILED_EVENTS_TOPIC", FAILED),
                Map.entry("FAILED_EVENTS_CONSUMER_GROUP", PROCESSOR_GROUP),
                Map.entry("FAILED_EVENTS_MAX_POLL_INTERVAL_MS", "30000"),
                Map.entry("FAILED_EVENTS_RETRY_INITIAL_INTERVAL", "500ms"),
                Map.entry("FAILED_EVENTS_RETRY_MAX_INTERVAL", "10s"),
                Map.entry("FAILED_EVENTS_RETRY_MULTIPLIER", "2"),
                Map.entry("FAILED_EVENTS_S3_ENDPOINT", MINIO.getS3URL()),
                Map.entry("FAILED_EVENTS_S3_REGION", "us-east-1"),
                Map.entry("FAILED_EVENTS_S3_ACCESS_KEY", MINIO.getUserName()),
                Map.entry("FAILED_EVENTS_S3_SECRET_KEY", MINIO.getPassword()),
                Map.entry("FAILED_EVENTS_S3_CONNECT_TIMEOUT", "2s"),
                Map.entry("FAILED_EVENTS_S3_API_CALL_TIMEOUT", "8s"),
                Map.entry("FAILED_EVENTS_S3_API_CALL_ATTEMPT_TIMEOUT", "4s"),
                // Клиент Schema Registry - как в .env.example (R25)
                Map.entry("FAILED_EVENTS_SCHEMA_REGISTRY_CONNECT_TIMEOUT_MS", "1000"),
                Map.entry("FAILED_EVENTS_SCHEMA_REGISTRY_READ_TIMEOUT_MS", "5000"),
                Map.entry("FAILED_EVENTS_SCHEMA_REGISTRY_MAX_RETRIES", "0"),
                Map.entry("FAILED_EVENTS_BUCKET", BUCKET),
                Map.entry("MANAGEMENT_OTLP_ENDPOINT", "http://localhost:4318/v1/traces"));
    }

    protected static String proxyUrl() {
        return "jdbc:postgresql://%s:%d/devices?connectTimeout=3&socketTimeout=15"
                .formatted(PROXY.getHost(), PROXY.getMappedPort(3307));
    }

    /** Заморозить контейнер (docker pause): соединения к нему зависают, адрес сохраняется. */
    protected static void pauseShard(GenericContainer<?> container) {
        DockerClientFactory.instance().client().pauseContainerCmd(container.getContainerId()).exec();
    }

    /** Снять паузу, если контейнер заморожен. */
    protected static void unpauseShard(GenericContainer<?> container) {
        String id = container.getContainerId();
        if (Boolean.TRUE.equals(DockerClientFactory.instance().client().inspectContainerCmd(id).exec()
                .getState().getPaused())) {
            DockerClientFactory.instance().client().unpauseContainerCmd(id).exec();
        }
    }

    /**
     * Proxy могли заморозить в тесте другого класса (контейнеры общие на JVM): снять паузу и дождаться, пока
     * Proxy отвечает на запрос к базе devices.
     */
    protected static void ensureProxyReady() {
        unpauseShard(PROXY);
        await().atMost(Duration.ofMinutes(3)).pollInterval(Duration.ofSeconds(2)).ignoreExceptions().untilAsserted(() -> {
            try (var connection = java.sql.DriverManager.getConnection(proxyUrl(), PROXY_USER, PROXY_PASSWORD);
                 var statement = connection.createStatement()) {
                statement.execute("SELECT 1");
            }
        });
    }

    // ------------------------------------------------------------------ Kafka
    // Как задача schema-register на стенде: в Schema Registry заранее лежит только схема failed-events-value (FailedEvent)
    private static void registerFailedEventsSchema() {
        try (var client = new CachedSchemaRegistryClient(schemaRegistryUrl(), 10)) {
            client.register(FAILED + "-value", new AvroSchema(FailedEvent.getClassSchema()));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to register the schema of " + FAILED + "-value", e);
        }
    }

    protected static String schemaRegistryUrl() {
        return "http://%s:%d".formatted(SCHEMA_REGISTRY.getHost(), SCHEMA_REGISTRY.getMappedPort(8081));
    }

    /** Avro через Schema Registry, как публикует events-collector-service; partition null - по ключу. */
    protected static RecordMetadata send(String key, Object value, Integer partition) throws Exception {
        try (var producer = new KafkaProducer<String, Object>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class,
                "schema.registry.url", schemaRegistryUrl()))) {
            return producer.send(new ProducerRecord<>(DEVICES, partition, key, value)).get();
        }
    }

    /** Avro-байты записи с настоящим schemaId: схема регистрируется под subject темы (topic-value), как у её продюсера. */
    protected static byte[] avroBytes(String topic, Object value) {
        try (var serializer = new KafkaAvroSerializer()) {
            serializer.configure(Map.of("schema.registry.url", schemaRegistryUrl()), false);
            return serializer.serialize(topic, value);
        }
    }

    protected static RecordMetadata sendRaw(String key, byte[] value, int partition) throws Exception {
        try (var producer = new KafkaProducer<String, byte[]>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            return producer.send(new ProducerRecord<>(DEVICES, partition, key, value)).get();
        }
    }

    /** Ждёт, пока сервис подтвердит offset сообщения: обработано (COMMIT в PostgreSQL) или записано в failed-events. */
    protected static void awaitCommitted(RecordMetadata message) {
        TopicPartition partition = new TopicPartition(message.topic(), message.partition());
        await().atMost(WAIT).untilAsserted(() -> assertThat(committedOffset(partition)).isGreaterThan(message.offset()));
    }

    private static long committedOffset(TopicPartition partition) throws Exception {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            OffsetAndMetadata committed = admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata().get()
                    .get(partition);
            return committed == null ? -1 : committed.offset();
        }
    }

    /**
     * Запись failed-events для исходного сообщения (ключ <topic>-<partition>-<offset>); ждёт её появления до WAIT
     * (запись - после повторов). Значение - FailedEvent, прочитанный через Schema Registry.
     */
    protected static Optional<FailedEvent> failedEvent(RecordMetadata original) {
        return await().atMost(WAIT).pollInterval(Duration.ofMillis(500))
                .until(() -> findFailedEvent(original), Optional::isPresent);
    }

    private static Optional<FailedEvent> findFailedEvent(RecordMetadata original) {
        String sourceKey = original.topic() + "-" + original.partition() + "-" + original.offset();
        try (var consumer = new KafkaConsumer<String, FailedEvent>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class,
                "schema.registry.url", schemaRegistryUrl(),
                "specific.avro.reader", true))) {
            List<TopicPartition> partitions = consumer.partitionsFor(FAILED).stream()
                    .map(info -> new TopicPartition(FAILED, info.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            List<ConsumerRecord<String, FailedEvent>> all = new ArrayList<>();
            // Читаем до конца разделов: число poll не зависит от скорости машины
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            while (partitions.stream().anyMatch(p -> consumer.position(p) < end.get(p))) {
                consumer.poll(Duration.ofMillis(500)).forEach(all::add);
            }
            return all.stream()
                    .filter(r -> sourceKey.equals(r.key()))
                    .map(ConsumerRecord::value)
                    .findFirst();
        }
    }

    /** Байты из ByteBuffer записи FailedEvent (key, value, headers) без лишних байт буфера. */
    protected static byte[] bytesOf(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    /** Настройка темы, как её видит брокер (для проверок cleanup.policy и retention.ms - FR-022, ADR-0011). */
    protected static String topicConfig(String topic, String name) {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            return admin.describeConfigs(List.of(resource)).all().get().get(resource).get(name).value();
        } catch (Exception e) {
            throw new IllegalStateException("Не удалось прочитать настройки темы " + topic, e);
        }
    }

    protected static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static void createTopics() {
        try {
            // Значение - как в .env.example (FR-022): тема devices создаётся с retention.ms и без сжатия
            // Тема failed-events - со значениями .env.example (имя и срок хранения): скрипт требует их без значений по умолчанию
            var result = KAFKA.execInContainer("bash", "-c", "KAFKA_BOOTSTRAP=" + KAFKA_INTERNAL
                    + " KAFKA_DEVICES_RETENTION_MS=2592000000"
                    + " KAFKA_FAILED_EVENTS_TOPIC=" + FAILED
                    + " KAFKA_FAILED_EVENTS_RETENTION_MS=1209600000 /opt/init/init-topics.sh");
            if (result.getExitCode() != 0) {
                throw new IllegalStateException("init-topics.sh exited with code " + result.getExitCode()
                        + ":\n" + result.getStdout() + result.getStderr());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create Kafka topics", e);
        }
    }

    // ------------------------------------------------------------------ шарды

    private static GenericContainer<?> shard(int index) {
        return new GenericContainer<>("postgres:18.6-alpine")
                .withNetwork(NETWORK)
                .withNetworkAliases("postgres-shard-" + index)
                .withExposedPorts(5432)
                .withEnv("POSTGRES_DB", "shard_" + index)
                .withEnv("POSTGRES_USER", SHARD_USER)
                .withEnv("POSTGRES_PASSWORD", SHARD_PASSWORD)
                .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2)
                        .withStartupTimeout(Duration.ofMinutes(1)));
    }

    private static GenericContainer<?> replica(int index) {
        return new GenericContainer<>("postgres:18.6-alpine")
                .withNetwork(NETWORK)
                .withNetworkAliases("postgres-shard-" + index + "-replica")
                .withExposedPorts(5432)
                .withEnv("POSTGRES_DB", "shard_" + index)
                .withEnv("POSTGRES_USER", SHARD_USER)
                .withEnv("POSTGRES_PASSWORD", SHARD_PASSWORD)
                .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2)
                        .withStartupTimeout(Duration.ofMinutes(1)));
    }

    /** Роль device_reader (LOGIN) на реплике - как shard-replication-init на стенде. */
    private static void createReaderRole(GenericContainer<?> replica, int index) {
        try {
            var result = replica.execInContainer("psql", "-U", SHARD_USER, "-d", "shard_" + index,
                    "-v", "ON_ERROR_STOP=1", "-c",
                    "CREATE ROLE device_reader WITH LOGIN PASSWORD '" + READER_PASSWORD + "'");
            if (result.getExitCode() != 0) {
                throw new IllegalStateException("CREATE ROLE device_reader failed: " + result.getStderr());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create device_reader on replica " + index, e);
        }
    }

    private static ShardTarget target(int index, GenericContainer<?> shard) {
        return new ShardTarget("shard_" + index, "jdbc:postgresql://%s:%d/shard_%d"
                .formatted(shard.getHost(), shard.getMappedPort(5432), index), SHARD_USER, SHARD_PASSWORD);
    }

    /** SQL прямо на физическом шарде (мимо Proxy) - проверить, где на самом деле лежит строка. */
    protected static String onShard(int index, String sql) throws Exception {
        GenericContainer<?> shard = index == 0 ? SHARD_0 : SHARD_1;
        var result = shard.execInContainer("psql", "-U", SHARD_USER, "-d", "shard_" + index, "-tA", "-F|", "-c", sql);
        assertThat(result.getExitCode()).as(result.getStderr()).isZero();
        return result.getStdout().strip();
    }

    /** Шард по правилу из database-devices.yaml: |hashCode| % 2. */
    protected static int shardIndexOf(String deviceId) {
        return Math.abs(deviceId.hashCode() % 2);
    }

    /**
     * Строка устройства есть на ожидаемом шарде (по правилу из database-devices.yaml) и ОТСУТСТВУЕТ на другом -
     * проверка напрямую в PostgreSQL, мимо Proxy. Возвращает индекс ожидаемого шарда.
     */
    protected static int assertOnlyOnExpectedShard(String deviceId) throws Exception {
        int expected = shardIndexOf(deviceId);
        int other = 1 - expected;
        String where = "SELECT count(*) FROM devices WHERE device_id = '" + deviceId + "'";
        assertThat(onShard(expected, where)).as("%s on expected shard_%d", deviceId, expected).isEqualTo("1");
        assertThat(onShard(other, where)).as("%s must not be on shard_%d", deviceId, other).isEqualTo("0");
        return expected;
    }

    protected static String deviceOnShard(int index) {
        while (true) {
            String id = "it-" + UUID.randomUUID();
            if (shardIndexOf(id) == index) {
                return id;
            }
        }
    }

    protected static void stopShard(GenericContainer<?> shard) {
        DockerClientFactory.instance().client().stopContainerCmd(shard.getContainerId()).exec();
    }

    protected static void startShard(GenericContainer<?> shard) {
        DockerClientFactory.instance().client().startContainerCmd(shard.getContainerId()).exec();
    }

    // ------------------------------------------------------------------ HTTP сервиса

    protected HttpResponse<String> get(String path) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(20)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }
}