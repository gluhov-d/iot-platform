package com.github.gluhov.iotplatform.devicecollector.it;

import com.github.gluhov.iotplatform.dbmigrator.DbMigrator;
import com.github.gluhov.iotplatform.dbmigrator.ShardTarget;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

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
    protected static final String DLT = "devices-dlt";
    protected static final String GROUP = "device-collector-it";
    protected static final Duration WAIT = Duration.ofSeconds(60);

    private static final Network NETWORK = Network.newNetwork();
    private static final String KAFKA_INTERNAL = "kafka:19092";
    private static final String SHARD_USER = "devices";
    // Пароли с кавычками, #, \, & и пробелом: render-config.sh должен подставить их в YAML Proxy без искажений
    private static final String SHARD_PASSWORD="it-shard p'a\"s#s\\&1";
    private static final String PROXY_USER = "devices_app";
    private static final String PROXY_PASSWORD="it-proxy p'a\"s#s\\&2";

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
            .withCopyFileToContainer(MountableFile.forHostPath(REPO_ROOT.resolve("infrastructure/config/shardingsphere")),
                    "/opt/init/conf")
            .withCopyFileToContainer(MountableFile.forHostPath(
                    REPO_ROOT.resolve("infrastructure/config/shardingsphere/render-config.sh"), 0755), "/opt/init/render-config.sh")
            .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("/bin/bash", "/opt/init/render-config.sh"))
            .waitingFor(Wait.forLogMessage(".*ShardingSphere-Proxy .* mode started successfully.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(3)));

    static {
        Startables.deepStart(KAFKA, SCHEMA_REGISTRY, SHARD_0, SHARD_1).join();
        createTopics();
        registerDltSchemas();
        // Как на стенде: схема на шардах до старта Proxy - Proxy читает метаданные таблиц при старте
        DbMigrator.migrateAll(List.of(target(0, SHARD_0), target(1, SHARD_1)));
        PROXY.start();
    }

    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Value("${local.server.port}")
    protected int port;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("KAFKA_BOOTSTRAP_SERVERS", KAFKA::getBootstrapServers);
        registry.add("SCHEMA_REGISTRY_URL", AbstractItTest::schemaRegistryUrl);
        // auto.register.schemas остаётся false, как на стенде: схему devices-dlt-value регистрирует registerDltSchemas()
        // (на стенде - задача schema-register), а не продюсер сервиса
        registry.add("DEVICES_DB_URL", () -> "jdbc:postgresql://%s:%d/devices?connectTimeout=3&socketTimeout=15"
                .formatted(PROXY.getHost(), PROXY.getMappedPort(3307)));
        registry.add("DEVICES_DB_USERNAME", () -> PROXY_USER);
        registry.add("DEVICES_DB_PASSWORD", () -> PROXY_PASSWORD);
        registry.add("DEVICE_COLLECTOR_CONSUMER_GROUP", () -> GROUP);
    }

    // ------------------------------------------------------------------ Kafka
    // Как задача schema-register на стенде: в Schema Registry заранее лежит только схема devices-dlt-value (DeviceInfo)
    private static void registerDltSchemas() {
        try (var client = new CachedSchemaRegistryClient(schemaRegistryUrl(), 10)) {
            client.register(DLT + "-value", new AvroSchema(DeviceInfo.getClassSchema()));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to register the schema of " + DLT + "-value", e);
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

    /** Ждёт, пока сервис подтвердит offset сообщения: обработано (COMMIT в PostgreSQL) или записано в devices-dlt. */
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

    /** Запись devices-dlt для исходного сообщения; ждёт её появления до WAIT (запись в DLT - после повторов). */
    protected static Optional<ConsumerRecord<String, byte[]>> deadLetter(RecordMetadata original) {
        return await().atMost(WAIT).pollInterval(Duration.ofMillis(500))
                .until(() -> findDeadLetter(original), Optional::isPresent);
    }

    private static Optional<ConsumerRecord<String, byte[]>> findDeadLetter(RecordMetadata original) {
        try (var consumer = new KafkaConsumer<String, byte[]>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class))) {
            List<TopicPartition> partitions = consumer.partitionsFor(DLT).stream()
                    .map(info -> new TopicPartition(DLT, info.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            List<ConsumerRecord<String, byte[]>> all = new ArrayList<>();
            // Читаем до конца разделов: число poll не зависит от скорости машины
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            while (partitions.stream().anyMatch(p -> consumer.position(p) < end.get(p))) {
                consumer.poll(Duration.ofMillis(500)).forEach(all::add);
            }
            return all.stream()
                    .filter(r -> longHeader(r, "kafka_dlt-original-offset") == original.offset()
                            && intHeader(r, "kafka_dlt-original-partition") == original.partition())
                    .findFirst();
        }
    }

    protected static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static long longHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? -1 : ByteBuffer.wrap(header.value()).getLong();
    }

    private static int intHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? -1 : ByteBuffer.wrap(header.value()).getInt();
    }

    private static void createTopics() {
        try {
            var result = KAFKA.execInContainer("bash", "-c", "KAFKA_BOOTSTRAP=" + KAFKA_INTERNAL + " /opt/init/init-topics.sh");
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