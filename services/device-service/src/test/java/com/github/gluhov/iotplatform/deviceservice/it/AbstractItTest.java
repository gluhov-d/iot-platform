package com.github.gluhov.iotplatform.deviceservice.it;

import com.github.gluhov.iotplatform.dbmigrator.DbMigrator;
import com.github.gluhov.iotplatform.dbmigrator.ShardTarget;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
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
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Стенд интеграционных тестов device-service: Kafka и Schema Registry (схема devices-value 1.3.0 регистрируется заранее,
 * как задача schema-register на стенде), Redis, PostgreSQL со схемой от db-migrator и Keycloak 26.0.8 с realm из
 * infrastructure/config/keycloak. Контейнеры поднимаются один раз на JVM; сервис получает адреса через @DynamicPropertySource.
 * Пока без ShardingSphere-Proxy: чтение из реплик появится с задачами US1 (T042).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
public abstract class AbstractItTest {

    protected static final Path REPO_ROOT = Path.of("../..").toAbsolutePath().normalize();
    protected static final String REALM = "iot-platform";
    protected static final String AUDIENCE = "device-service";
    protected static final Duration WAIT = Duration.ofSeconds(60);

    // Секреты и пароли создаются при каждом запуске, в коде их нет
    protected static final String KEYCLOAK_ADMIN = "it-admin";
    protected static final String KEYCLOAK_ADMIN_PASSWORD = UUID.randomUUID().toString();
    protected static final String CONSUMER_SECRET = UUID.randomUUID().toString();
    protected static final String READER_SECRET = UUID.randomUUID().toString();

    private static final Network NETWORK = Network.newNetwork();
    private static final String KAFKA_INTERNAL = "kafka:19092";
    private static final String DB_USER = "devices";
    private static final String DB_PASSWORD = UUID.randomUUID().toString();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

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

    protected static final GenericContainer<?> POSTGRES = new GenericContainer<>("postgres:18.6-alpine")
            .withNetwork(NETWORK)
            .withExposedPorts(5432)
            .withEnv("POSTGRES_DB", "shard_0")
            .withEnv("POSTGRES_USER", DB_USER)
            .withEnv("POSTGRES_PASSWORD", DB_PASSWORD)
            .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*", 2)
                    .withStartupTimeout(Duration.ofMinutes(1)));

    protected static final GenericContainer<?> REDIS = new GenericContainer<>("redis:8.6.7-alpine")
            .withNetwork(NETWORK)
            .withExposedPorts(6379)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(1)));

    // Keycloak стартует с realm из репозитория; адрес (iss) - тот, по которому тесты обращаются к нему с хоста
    protected static final GenericContainer<?> KEYCLOAK = new GenericContainer<>("quay.io/keycloak/keycloak:26.0.8")
            .withNetwork(NETWORK)
            .withExposedPorts(8080)
            .withEnv("KC_BOOTSTRAP_ADMIN_USERNAME", KEYCLOAK_ADMIN)
            .withEnv("KC_BOOTSTRAP_ADMIN_PASSWORD", KEYCLOAK_ADMIN_PASSWORD)
            .withEnv("KC_DB", "dev-file")
            .withEnv("KC_HTTP_ENABLED", "true")
            .withEnv("KC_HOSTNAME_STRICT", "false")
            .withEnv("DEVICE_SERVICE_CONSUMER_SECRET", CONSUMER_SECRET)
            .withEnv("DEVICE_SERVICE_READER_SECRET", READER_SECRET)
            .withEnv("JAVA_OPTS_APPEND", "-Xms256m -Xmx512m")
            .withCommand("start-dev", "--import-realm", "--http-port=8080", "--health-enabled=true")
            // Режим задаём явно: MountableFile переносит права файла рабочего дерева, а Keycloak читает импорт
            // под пользователем keycloak - файл с правами 600 даёт «Failed to run import» и выход с кодом 1
            .withCopyFileToContainer(MountableFile.forHostPath(
                    REPO_ROOT.resolve("infrastructure/config/keycloak/iot-platform-realm.json"), 0644),
                    "/opt/keycloak/data/import/iot-platform-realm.json")
            .waitingFor(Wait.forHttp("/realms/" + REALM).forPort(8080).forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(3)));

    static {
        Startables.deepStart(KAFKA, SCHEMA_REGISTRY, POSTGRES, REDIS, KEYCLOAK).join();
        createTopics();
        registerDeviceSchema();
        DbMigrator.migrateAll(List.of(new ShardTarget("shard_0", jdbcUrl(), DB_USER, DB_PASSWORD)));
    }

    @Value("${local.server.port}")
    protected int port;

    // Management-порт: на случайном свободном порту (application-test.yaml, management.server.port: 0)
    @Value("${local.management.port}")
    protected int managementPort;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("KAFKA_BOOTSTRAP_SERVERS", KAFKA::getBootstrapServers);
        registry.add("SCHEMA_REGISTRY_URL", AbstractItTest::schemaRegistryUrl);
        registry.add("spring.datasource.url", AbstractItTest::jdbcUrl);
        registry.add("spring.datasource.username", () -> DB_USER);
        registry.add("spring.datasource.password", () -> DB_PASSWORD);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("KEYCLOAK_ISSUER_URI", () -> keycloakUrl() + "/realms/" + REALM);
        registry.add("KEYCLOAK_JWK_SET_URI", () -> keycloakUrl() + "/realms/" + REALM + "/protocol/openid-connect/certs");
        registry.add("DEVICE_SERVICE_AUDIENCE", () -> AUDIENCE);
    }

    /** Адрес Keycloak с хоста тестов: тот же, что видит сервис (один iss для токенов и проверки). */
    public static String keycloakUrl() {
        return "http://%s:%d".formatted(KEYCLOAK.getHost(), KEYCLOAK.getMappedPort(8080));
    }

    private static String jdbcUrl() {
        return "jdbc:postgresql://%s:%d/shard_0".formatted(POSTGRES.getHost(), POSTGRES.getMappedPort(5432));
    }

    private static String schemaRegistryUrl() {
        return "http://%s:%d".formatted(SCHEMA_REGISTRY.getHost(), SCHEMA_REGISTRY.getMappedPort(8081));
    }

    private static void createTopics() {
        try {
            var result = KAFKA.execInContainer("bash", "-c", "KAFKA_BOOTSTRAP=" + KAFKA_INTERNAL
                    + " KAFKA_DEVICES_RETENTION_MS=2592000000 /opt/init/init-topics.sh");
            if (result.getExitCode() != 0) {
                throw new IllegalStateException("init-topics.sh exited with code " + result.getExitCode()
                        + ":\n" + result.getStdout() + result.getStderr());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create Kafka topics", e);
        }
    }

    // Как задача schema-register на стенде: схема devices-value 1.3.0 лежит в реестре заранее
    private static void registerDeviceSchema() {
        try (var client = new CachedSchemaRegistryClient(schemaRegistryUrl(), 10)) {
            client.register("devices-value", new AvroSchema(DeviceInfo.getClassSchema()));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to register the schema of devices-value", e);
        }
    }

    /** Запрос к сервису с Bearer-токеном (null - без заголовка Authorization); для POST и PUT тело - пустой JSON. */
    protected HttpResponse<String> send(String method, String path, String bearer) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(20));
        if (bearer != null) {
            builder.header("Authorization", "Bearer " + bearer);
        }
        boolean hasBody = "PUT".equals(method) || "POST".equals(method);
        builder.method(method, hasBody ? HttpRequest.BodyPublishers.ofString("{}") : HttpRequest.BodyPublishers.noBody());
        if (hasBody) {
            builder.header("Content-Type", "application/json");
        }
        return HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }
}
