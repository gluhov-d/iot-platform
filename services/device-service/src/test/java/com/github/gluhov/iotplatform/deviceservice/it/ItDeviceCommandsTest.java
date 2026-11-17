package com.github.gluhov.iotplatform.deviceservice.it;

import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.events.avro.DeviceStatus;
import com.github.gluhov.iotplatform.events.avro.DeviceUpsertEventType;
import com.github.gluhov.iotplatform.events.avro.OperationType;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.DockerClientFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Команды администратора (US2–US4) с настоящими Kafka, Schema Registry и PostgreSQL. Сообщения читаются потребителем
 * темы devices (десериализация Avro через Schema Registry): ключ, operation, eventType, одно сообщение на команду.
 * Идентификаторы устройств у каждого теста свои: контейнеры общие для всех классов на JVM.
 */
class ItDeviceCommandsTest extends AbstractItTest {

    private static final String TOPIC = "devices";
    /** Ожидание первого сообщения и тишина после него: дубль, если он есть, придёт за это время */
    private static final Duration QUIET = Duration.ofSeconds(3);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static String consumerToken;

    @Autowired
    private JdbcClient jdbc;

    @BeforeAll
    static void obtainConsumerToken() throws Exception {
        // Сервисная учётная запись с devices.read и devices.write (realm, роль в roles.client)
        consumerToken = new KeycloakTokens(keycloakUrl(), KEYCLOAK_ADMIN, KEYCLOAK_ADMIN_PASSWORD)
                .clientCredentials("device-service-consumer", CONSUMER_SECRET);
    }

    @Test
    void givenDevice_whenPut_thenUpdateCommandPublished() throws Exception {
        String id = newId();
        insertDevice(id, "t1", 3, false);

        HttpResponse<String> response;
        List<ConsumerRecord<String, DeviceInfo>> messages;
        try (KafkaConsumer<String, DeviceInfo> consumer = devicesConsumer()) {
            response = put(id, UPDATE_BODY);
            messages = awaitMessages(consumer, id);
        }

        assertThat(response.statusCode()).isEqualTo(202);
        assertThat(response.headers().firstValue("Location")).hasValue("/api/v1/devices/" + id);
        JsonNode body = JSON.readTree(response.body());
        assertThat(body.get("deviceId").asString()).isEqualTo(id);
        assertThat(body.get("operation").asString()).isEqualTo("UPDATE");
        UUID commandId = UUID.fromString(body.get("commandId").asString());
        assertThat(messages).hasSize(1);
        ConsumerRecord<String, DeviceInfo> record = messages.get(0);
        assertThat(record.key()).isEqualTo(id);
        DeviceInfo message = record.value();
        assertThat(message.getOperation()).isEqualTo(OperationType.UPDATE);
        assertThat(message.getEventType()).isEqualTo(DeviceUpsertEventType.UPDATED);
        assertThat(message.getEventId()).isEqualTo(commandId.toString());
        assertThat(message.getDevice().getTenantId()).isEqualTo("t1");
        assertThat(message.getDevice().getOwnerId()).isEqualTo("owner-2");
        assertThat(message.getDevice().getStatus()).isEqualTo(DeviceStatus.ACTIVE);
    }

    @Test
    void givenInvalidBody_whenPut_thenBadRequestAndNoMessage() throws Exception {
        String id = newId();
        insertDevice(id, "t1", 1, false);
        // Нет обязательного tenantId
        String invalid = "{\"deviceType\":\"sensor\",\"status\":\"ACTIVE\"}";

        HttpResponse<String> response;
        List<ConsumerRecord<String, DeviceInfo>> messages;
        try (KafkaConsumer<String, DeviceInfo> consumer = devicesConsumer()) {
            response = put(id, invalid);
            messages = collect(consumer, id, QUIET);
        }

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("\"code\":\"VALIDATION_FAILED\"");
        assertThat(messages).isEmpty();
    }

    @Test
    void givenNoDevice_whenPut_thenNotFoundAndNoMessage() throws Exception {
        String id = newId();

        HttpResponse<String> response;
        List<ConsumerRecord<String, DeviceInfo>> messages;
        try (KafkaConsumer<String, DeviceInfo> consumer = devicesConsumer()) {
            response = put(id, UPDATE_BODY);
            messages = collect(consumer, id, QUIET);
        }

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.body()).contains("\"code\":\"DEVICE_NOT_FOUND\"");
        assertThat(messages).isEmpty();
    }

    @Test
    void givenKafkaDown_whenPut_thenCommandNotAccepted() throws Exception {
        String id = newId();
        insertDevice(id, "t1", 1, false);
        String kafkaId = KAFKA.getContainerId();
        HttpResponse<String> response;
        long started = System.nanoTime();
        // Пауза, а не остановка: порт брокера не меняется. Снимается в finally, чтобы не задеть другие тесты класса
        DockerClientFactory.instance().client().pauseContainerCmd(kafkaId).exec();
        try {
            response = put(id, UPDATE_BODY);
        } finally {
            DockerClientFactory.instance().client().unpauseContainerCmd(kafkaId).exec();
        }
        Duration elapsed = Duration.ofNanos(System.nanoTime() - started);

        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(response.body()).contains("\"code\":\"COMMAND_NOT_ACCEPTED\"");
        // Срок отправки в тестовом профиле - 2 с; запас на HTTP и прогрев. Раньше ответ ждал 60 с (наблюдение Kafka)
        assertThat(elapsed).isLessThan(Duration.ofSeconds(6));
    }

    @Test
    void givenNoDevice_whenPost_thenCreateCommandPublished() throws Exception {
        String id = newId();

        HttpResponse<String> response;
        List<ConsumerRecord<String, DeviceInfo>> messages;
        try (KafkaConsumer<String, DeviceInfo> consumer = devicesConsumer()) {
            response = sendJson("POST", "/api/v1/devices", createBody(id));
            messages = awaitMessages(consumer, id);
        }

        assertThat(response.statusCode()).isEqualTo(202);
        assertThat(response.headers().firstValue("Location")).hasValue("/api/v1/devices/" + id);
        JsonNode body = JSON.readTree(response.body());
        assertThat(body.get("operation").asString()).isEqualTo("CREATE");
        assertThat(messages).hasSize(1);
        DeviceInfo message = messages.get(0).value();
        assertThat(messages.get(0).key()).isEqualTo(id);
        assertThat(message.getOperation()).isEqualTo(OperationType.CREATE);
        assertThat(message.getEventType()).isEqualTo(DeviceUpsertEventType.CREATED);
        assertThat(message.getEventId()).isEqualTo(body.get("commandId").asString());
        assertThat(message.getDevice().getDeviceId()).isEqualTo(id);
        assertThat(message.getDevice().getStatus()).isEqualTo(DeviceStatus.ACTIVE);
    }

    @Test
    void givenDevice_whenPost_thenConflictAndNoMessage() throws Exception {
        String id = newId();
        insertDevice(id, "t1", 1, false);

        HttpResponse<String> response;
        List<ConsumerRecord<String, DeviceInfo>> messages;
        try (KafkaConsumer<String, DeviceInfo> consumer = devicesConsumer()) {
            response = sendJson("POST", "/api/v1/devices", createBody(id));
            messages = collect(consumer, id, QUIET);
        }

        assertThat(response.statusCode()).isEqualTo(409);
        assertThat(response.body()).contains("\"code\":\"DEVICE_ALREADY_EXISTS\"");
        assertThat(messages).isEmpty();
    }

    @Test
    void givenNoDeviceIdOrTooLong_whenPost_thenBadRequest() throws Exception {
        String withoutId = UPDATE_BODY;
        String tooLong = createBody("d".repeat(129));

        HttpResponse<String> missing = sendJson("POST", "/api/v1/devices", withoutId);
        HttpResponse<String> long129 = sendJson("POST", "/api/v1/devices", tooLong);

        assertThat(missing.statusCode()).isEqualTo(400);
        assertThat(missing.body()).contains("\"code\":\"VALIDATION_FAILED\"");
        assertThat(long129.statusCode()).isEqualTo(400);
        assertThat(long129.body()).contains("\"code\":\"VALIDATION_FAILED\"");
    }

    @Test
    void givenDevice_whenDelete_thenDeleteCommandPublished() throws Exception {
        String id = newId();
        insertDevice(id, "t1", 2, false);

        HttpResponse<String> response;
        List<ConsumerRecord<String, DeviceInfo>> messages;
        try (KafkaConsumer<String, DeviceInfo> consumer = devicesConsumer()) {
            response = sendJson("DELETE", "/api/v1/devices/" + id, null);
            messages = awaitMessages(consumer, id);
        }

        assertThat(response.statusCode()).isEqualTo(202);
        assertThat(response.headers().firstValue("Location")).hasValue("/api/v1/devices/" + id);
        JsonNode body = JSON.readTree(response.body());
        assertThat(body.get("operation").asString()).isEqualTo("DELETE");
        assertThat(messages).hasSize(1);
        DeviceInfo message = messages.get(0).value();
        assertThat(messages.get(0).key()).isEqualTo(id);
        assertThat(message.getOperation()).isEqualTo(OperationType.DELETE);
        assertThat(message.getEventType()).isEqualTo(DeviceUpsertEventType.DELETED);
        assertThat(message.getEventId()).isEqualTo(body.get("commandId").asString());
        // У DELETE снимка устройства нет: поля администратора в команде не передаются
        assertThat(message.getDevice()).isNull();
    }

    @Test
    void givenNoDevice_whenDelete_thenNotFoundAndNoMessage() throws Exception {
        String id = newId();

        HttpResponse<String> response;
        List<ConsumerRecord<String, DeviceInfo>> messages;
        try (KafkaConsumer<String, DeviceInfo> consumer = devicesConsumer()) {
            response = sendJson("DELETE", "/api/v1/devices/" + id, null);
            messages = collect(consumer, id, QUIET);
        }

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.body()).contains("\"code\":\"DEVICE_NOT_FOUND\"");
        assertThat(messages).isEmpty();
    }

    /** Тело создания: deviceId и поля администратора из UPDATE_BODY. */
    private static String createBody(String deviceId) {
        return "{\"deviceId\":\"" + deviceId + "\"," + UPDATE_BODY.substring(1);
    }

    private static final String UPDATE_BODY = "{\"name\":\"sensor-name\",\"tenantId\":\"t1\",\"ownerId\":\"owner-2\","
            + "\"deviceType\":\"sensor\",\"serialNumber\":\"SN-2\",\"manufacturer\":\"ACME\",\"model\":\"T200\","
            + "\"status\":\"ACTIVE\",\"settings\":{\"mode\":\"eco\"},\"attributes\":{\"zone\":\"B\"}}";

    private HttpResponse<String> put(String id, String body) throws Exception {
        return sendJson("PUT", "/api/v1/devices/" + id, body);
    }

    private HttpResponse<String> sendJson(String method, String path, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(30))
                .header("Authorization", "Bearer " + consumerToken)
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(body))
                .build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    }

    /** Потребитель темы devices с отдельной группой: читает с начала темы, значения - через Schema Registry. */
    private static KafkaConsumer<String, DeviceInfo> devicesConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "it-commands-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put("schema.registry.url", "http://%s:%d".formatted(SCHEMA_REGISTRY.getHost(),
                SCHEMA_REGISTRY.getMappedPort(8081)));
        properties.put("specific.avro.reader", true);
        KafkaConsumer<String, DeviceInfo> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(TOPIC));
        return consumer;
    }

    /** Ждёт первое сообщение с ключом deviceId, затем ещё QUIET - чтобы поймать дубль. */
    private static List<ConsumerRecord<String, DeviceInfo>> awaitMessages(KafkaConsumer<String, DeviceInfo> consumer,
                                                                         String deviceId) {
        List<ConsumerRecord<String, DeviceInfo>> found = new ArrayList<>();
        Instant deadline = Instant.now().plus(WAIT);
        while (found.isEmpty() && Instant.now().isBefore(deadline)) {
            found.addAll(matching(consumer.poll(Duration.ofMillis(500)), deviceId));
        }
        found.addAll(collect(consumer, deviceId, QUIET));
        return found;
    }

    /** Сообщения с ключом deviceId за указанное время опроса. */
    private static List<ConsumerRecord<String, DeviceInfo>> collect(KafkaConsumer<String, DeviceInfo> consumer,
                                                                   String deviceId, Duration duration) {
        List<ConsumerRecord<String, DeviceInfo>> found = new ArrayList<>();
        Instant until = Instant.now().plus(duration);
        while (Instant.now().isBefore(until)) {
            found.addAll(matching(consumer.poll(Duration.ofMillis(300)), deviceId));
        }
        return found;
    }

    private static List<ConsumerRecord<String, DeviceInfo>> matching(ConsumerRecords<String, DeviceInfo> records,
                                                                    String deviceId) {
        List<ConsumerRecord<String, DeviceInfo>> found = new ArrayList<>();
        for (ConsumerRecord<String, DeviceInfo> record : records) {
            if (deviceId.equals(record.key())) {
                found.add(record);
            }
        }
        return found;
    }

    private static String newId() {
        return "it-cmd-" + UUID.randomUUID();
    }

    /** Строка шарда напрямую (как запись device-collector-service); карточка есть, если tenant != null. */
    private void insertDevice(String id, String tenant, long version, boolean deleted) {
        jdbc.sql("""
                        INSERT INTO devices (device_id, name, tenant_id, owner_id, device_type, serial_number, manufacturer,
                            model, status, settings, attributes, firmware_version, os_name, os_version, sensors,
                            registered_at, last_seen_at, version, seen_count, first_seen_at, last_event_at,
                            last_event_type, deleted_at)
                        VALUES (:id, 'sensor-name', :tenant, 'owner-1', 'sensor', 'SN-1', 'ACME', 'T100', 'ACTIVE',
                            CAST(:settings AS jsonb), CAST(:attributes AS jsonb), '1.0.0', 'Linux', '6.1',
                            CAST('[]' AS jsonb), now(), now(), :version, 1, now(), now(), 'CREATED',
                            CASE WHEN :deleted THEN now() ELSE NULL END)""")
                .param("id", id)
                .param("tenant", tenant, Types.VARCHAR)
                .param("settings", "{\"mode\":\"eco\"}")
                .param("attributes", "{\"zone\":\"A\"}")
                .param("version", version)
                .param("deleted", deleted)
                .update();
    }
}
