package com.github.gluhov.iotplatform.deviceservice.it;

import com.github.gluhov.iotplatform.deviceservice.domain.port.out.DeviceReadRepository;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.DockerClientFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.net.http.HttpResponse;
import java.sql.Types;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Чтение устройства (US1) с настоящими Redis и PostgreSQL: кэш device:v1:{deviceId}, правило «карточка есть»,
 * порог device:v1:floor:{deviceId} от device-collector-service (живёт дольше кэша), отказ кэша и контракт ответа.
 * Идентификаторы устройств у каждого теста свои: контейнеры общие для всех классов на JVM.
 */
class ItDeviceReadTest extends AbstractItTest {

    /** Значение DEVICE_CACHE_TTL из application-test.yaml (10 минут) */
    private static final long TTL_MS = TimeUnit.MINUTES.toMillis(10);
    private static final Set<String> CONTRACT_FIELDS = Set.of("deviceId", "name", "tenantId", "ownerId", "deviceType",
            "serialNumber", "manufacturer", "model", "status", "settings", "attributes", "firmwareVersion", "osName",
            "osVersion", "sensors", "registeredAt", "lastSeenAt", "version");
    private static final Set<String> CONTRACT_SENSOR_FIELDS = Set.of("sensorId", "sensorType", "unit", "value",
            "measuredAt", "status");
    private static final ObjectMapper JSON = new ObjectMapper();

    private static String readerToken;

    @Autowired
    private JdbcClient jdbc;

    @Autowired
    private StringRedisTemplate redis;

    @MockitoSpyBean
    private DeviceReadRepository readRepository;

    @BeforeAll
    static void obtainReaderToken() throws Exception {
        readerToken = new KeycloakTokens(keycloakUrl(), KEYCLOAK_ADMIN, KEYCLOAK_ADMIN_PASSWORD)
                .clientCredentials("device-service-reader", READER_SECRET);
    }

    @Test
    void givenDevice_whenFirstGet_thenCachedWithTtl() throws Exception {
        String id = newId();
        insertDevice(id, "t1", 3, false);

        HttpResponse<String> response = get(id);

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(JSON.readTree(response.body()).get("version").asLong()).isEqualTo(3L);
        String key = cacheKey(id);
        assertThat(redis.opsForHash().get(key, "body")).isNotNull();
        assertThat(redis.opsForHash().get(key, "version")).isEqualTo("3");
        Long ttl = redis.getExpire(key, TimeUnit.MILLISECONDS);
        assertThat(ttl).isNotNull().isPositive().isLessThanOrEqualTo(TTL_MS);
    }

    @Test
    void givenCached_whenGet_thenNoDbQuery() throws Exception {
        String id = newId();
        insertDevice(id, "t1", 1, false);
        assertThat(get(id).statusCode()).isEqualTo(200);
        clearInvocations(readRepository);

        HttpResponse<String> second = get(id);

        assertThat(second.statusCode()).isEqualTo(200);
        verify(readRepository, never()).findById(anyString());
    }

    @Test
    void givenNoDeviceOrDeletedOrNoCard_whenGet_thenNotFound() throws Exception {
        String missing = newId();
        String deleted = newId();
        insertDevice(deleted, "t1", 2, true);
        String noCard = newId();
        insertDevice(noCard, null, 2, false);

        for (String id : new String[]{missing, deleted, noCard}) {
            HttpResponse<String> response = get(id);

            assertThat(response.statusCode()).as(id).isEqualTo(404);
            assertThat(response.body()).contains("\"code\":\"DEVICE_NOT_FOUND\"");
            assertThat(redis.hasKey(cacheKey(id))).as(id).isFalse();
        }
    }

    @Test
    void givenFloorAboveReplicaVersion_whenGet_thenNotCached() throws Exception {
        String id = newId();
        insertDevice(id, "t1", 3, false);
        // Порог выставил device-collector-service в своём ключе: версия реплики (3) ниже - в кэш не пишем
        redis.opsForValue().set(floorKey(id), "5", Duration.ofHours(24));

        HttpResponse<String> response = get(id);

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(redis.hasKey(cacheKey(id))).isFalse();
        assertThat(redis.opsForValue().get(floorKey(id))).isEqualTo("5");
    }

    @Test
    void givenFloorOutlivesCacheTtl_whenReplicaStale_thenStillNotCached() throws Exception {
        String id = newId();
        insertDevice(id, "t1", 3, false);
        // Порог живёт дольше кэша (DEVICE_CACHE_FLOOR_TTL = 24 ч против TTL кэша 10 мин): истечение ключа кэша
        // его не уносит, поэтому отставание реплики дольше TTL не возвращает старые данные в кэш
        redis.opsForValue().set(floorKey(id), "5", Duration.ofHours(24));

        assertThat(get(id).statusCode()).isEqualTo(200);
        Long floorTtl = redis.getExpire(floorKey(id), TimeUnit.MILLISECONDS);

        assertThat(floorTtl).isNotNull().isGreaterThan(TTL_MS);
        assertThat(redis.hasKey(cacheKey(id))).as("ключа кэша нет: версия ниже порога").isFalse();
    }

    @Test
    void givenReplicaCaughtUp_whenGet_thenCachedAndFloorRemoved() throws Exception {
        String id = newId();
        insertDevice(id, "t1", 5, false);
        redis.opsForValue().set(floorKey(id), "5", Duration.ofHours(24));

        assertThat(get(id).statusCode()).isEqualTo(200);

        assertThat(redis.opsForHash().get(cacheKey(id), "version")).isEqualTo("5");
        assertThat(redis.hasKey(floorKey(id))).as("реплика догнала - порог снят").isFalse();
    }

    @Test
    void givenRedisDown_whenGet_thenOk() throws Exception {
        String id = newId();
        insertDevice(id, "t1", 1, false);
        String redisId = REDIS.getContainerId();
        // Пауза, а не остановка: порт узла не меняется. Снимается в finally, чтобы не задеть другие тесты класса
        DockerClientFactory.instance().client().pauseContainerCmd(redisId).exec();
        HttpResponse<String> response;
        try {
            response = get(id);
        } finally {
            DockerClientFactory.instance().client().unpauseContainerCmd(redisId).exec();
        }

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(JSON.readTree(response.body()).get("deviceId").asString()).isEqualTo(id);
    }

    @Test
    void givenRedisDownAndNoDevice_whenGet_thenNotFound() throws Exception {
        // Замечание ревью: при недоступном Redis отсутствующее устройство - тот же 404, а не 200
        String missing = newId();
        String deleted = newId();
        insertDevice(deleted, "t1", 2, true);
        String redisId = REDIS.getContainerId();
        DockerClientFactory.instance().client().pauseContainerCmd(redisId).exec();
        try {
            for (String id : new String[]{missing, deleted}) {
                HttpResponse<String> response = get(id);

                assertThat(response.statusCode()).as(id).isEqualTo(404);
                assertThat(response.body()).contains("\"code\":\"DEVICE_NOT_FOUND\"");
            }
        } finally {
            DockerClientFactory.instance().client().unpauseContainerCmd(redisId).exec();
        }
    }

    @Test
    void givenDevice_whenGet_thenOnlyContractFields() throws Exception {
        String id = newId();
        insertDevice(id, "t1", 4, false);

        JsonNode body = JSON.readTree(get(id).body());

        assertThat(fieldNames(body)).isSubsetOf(CONTRACT_FIELDS);
        assertThat(fieldNames(body)).contains("deviceId", "tenantId", "deviceType", "status", "settings",
                "attributes", "sensors", "version");
        JsonNode sensor = body.get("sensors").get(0);
        assertThat(fieldNames(sensor)).isSubsetOf(CONTRACT_SENSOR_FIELDS);
    }

    private HttpResponse<String> get(String id) throws Exception {
        return send("GET", "/api/v1/devices/" + id, readerToken);
    }

    private static String newId() {
        return "it-read-" + UUID.randomUUID();
    }

    private static String cacheKey(String id) {
        return "device:v1:" + id;
    }

    /** Порог версии - отдельный ключ со своим сроком жизни (ADR-0003). */
    private static String floorKey(String id) {
        return "device:v1:floor:" + id;
    }

    private static Set<String> fieldNames(JsonNode node) {
        Set<String> names = new HashSet<>();
        node.propertyNames().forEach(names::add);
        return names;
    }

    /** Строка шарда напрямую (как запись device-collector-service); tenant == null - карточки нет. */
    private void insertDevice(String id, String tenant, long version, boolean deleted) {
        jdbc.sql("""
                        INSERT INTO devices (device_id, name, tenant_id, owner_id, device_type, serial_number, manufacturer,
                            model, status, settings, attributes, firmware_version, os_name, os_version, sensors,
                            registered_at, last_seen_at, version, seen_count, first_seen_at, last_event_at,
                            last_event_type, deleted_at)
                        VALUES (:id, 'sensor-name', :tenant, 'owner-1', 'sensor', 'SN-1', 'ACME', 'T100', 'ACTIVE',
                            CAST(:settings AS jsonb), CAST(:attributes AS jsonb), '1.0.0', 'Linux', '6.1',
                            CAST(:sensors AS jsonb), now(), now(), :version, 1, now(), now(), 'CREATED',
                            CASE WHEN :deleted THEN now() ELSE NULL END)""")
                .param("id", id)
                .param("tenant", tenant, Types.VARCHAR)
                .param("settings", "{\"mode\":\"eco\"}")
                .param("attributes", "{\"zone\":\"A\"}")
                .param("sensors", "[{\"sensorId\":\"s1\",\"sensorType\":\"temperature\",\"unit\":\"C\","
                        + "\"value\":\"21.5\",\"measuredAtMs\":1700000000000,\"status\":\"OK\"}]")
                .param("version", version)
                .param("deleted", deleted)
                .update();
    }
}
