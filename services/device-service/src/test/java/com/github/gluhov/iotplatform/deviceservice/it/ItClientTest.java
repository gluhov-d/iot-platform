package com.github.gluhov.iotplatform.deviceservice.it;

import com.github.gluhov.iotplatform.deviceservice.api.DevicesApi;
import com.github.gluhov.iotplatform.deviceservice.api.model.DeviceCommandAccepted;
import com.github.gluhov.iotplatform.deviceservice.api.model.DeviceCreateRequest;
import com.github.gluhov.iotplatform.deviceservice.api.model.DeviceResponse;
import com.github.gluhov.iotplatform.deviceservice.api.model.DeviceUpdateRequest;
import com.github.gluhov.iotplatform.deviceservice.api.model.ManagedStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.support.RestClientAdapter;
import org.springframework.web.service.invoker.HttpServiceProxyFactory;

import java.sql.Types;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Клиентская библиотека device-service-client (FR-012) против настоящего сервиса на RANDOM_PORT: DevicesApi через
 * HttpServiceProxyFactory и RestClient, токен Keycloak получен по client credentials. Проверяется то, что видит потребитель
 * библиотеки: 202 и Location, тело команды, ответ GET, исключение на 404 и 403 без нужной роли.
 * Идентификаторы устройств у каждого теста свои: контейнеры общие для всех классов на JVM.
 */
class ItClientTest extends AbstractItTest {

    private static String consumerToken;
    private static String readerToken;

    @Autowired
    private JdbcClient jdbc;

    @BeforeAll
    static void obtainTokens() throws Exception {
        KeycloakTokens keycloak = new KeycloakTokens(keycloakUrl(), KEYCLOAK_ADMIN, KEYCLOAK_ADMIN_PASSWORD);
        // Сервисная учётная запись с devices.read и devices.write
        consumerToken = keycloak.clientCredentials("device-service-consumer", CONSUMER_SECRET);
        // Сервисная учётная запись только с devices.read
        readerToken = keycloak.clientCredentials("device-service-reader", READER_SECRET);
    }

    @Test
    void givenNewDevice_whenCreateDevice_thenAcceptedWithLocationAndBody() {
        String id = newId();

        var response = api(consumerToken).createDevice(createRequest(id));

        assertThat(response.getStatusCode().value()).isEqualTo(202);
        assertThat(response.getHeaders().getFirst("Location")).isEqualTo("/api/v1/devices/" + id);
        DeviceCommandAccepted body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.getDeviceId()).isEqualTo(id);
        assertThat(body.getOperation()).isEqualTo(DeviceCommandAccepted.OperationEnum.CREATE);
        assertThat(body.getCommandId()).isNotNull();
    }

    @Test
    void givenDeviceInReadModel_whenGetDeviceById_thenReturnsDevice() {
        String id = newId();
        insertDevice(id, "t1", 3);

        DeviceResponse device = api(readerToken).getDeviceById(id).getBody();

        assertThat(device).isNotNull();
        assertThat(device.getDeviceId()).isEqualTo(id);
        assertThat(device.getTenantId()).isEqualTo("t1");
        assertThat(device.getVersion()).isEqualTo(3L);
    }

    @Test
    void givenDeviceInReadModel_whenUpdateDevice_thenAcceptedWithUpdateOperation() {
        String id = newId();
        insertDevice(id, "t1", 3);

        var response = api(consumerToken).updateDevice(id, updateRequest());

        assertThat(response.getStatusCode().value()).isEqualTo(202);
        assertThat(response.getHeaders().getFirst("Location")).isEqualTo("/api/v1/devices/" + id);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getDeviceId()).isEqualTo(id);
        assertThat(response.getBody().getOperation()).isEqualTo(DeviceCommandAccepted.OperationEnum.UPDATE);
    }

    @Test
    void givenDeviceInReadModel_whenDeleteDevice_thenAcceptedWithDeleteOperation() {
        String id = newId();
        insertDevice(id, "t1", 3);

        var response = api(consumerToken).deleteDevice(id);

        assertThat(response.getStatusCode().value()).isEqualTo(202);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getOperation()).isEqualTo(DeviceCommandAccepted.OperationEnum.DELETE);
    }

    @Test
    void givenUnknownDevice_whenGetDeviceById_thenNotFoundException() {
        String missing = newId();

        assertThatThrownBy(() -> api(readerToken).getDeviceById(missing))
                .isInstanceOfSatisfying(HttpClientErrorException.NotFound.class, e ->
                        assertThat(e.getResponseBodyAsString()).contains("\"code\":\"DEVICE_NOT_FOUND\""));
    }

    @Test
    void givenReaderToken_whenCreateDevice_thenForbiddenException() {
        assertThatThrownBy(() -> api(readerToken).createDevice(createRequest(newId())))
                .isInstanceOfSatisfying(HttpClientErrorException.Forbidden.class, e ->
                        assertThat(e.getResponseBodyAsString()).contains("\"code\":\"FORBIDDEN\""));
    }

    /** Клиент DevicesApi поверх RestClient с Bearer-токеном: так же, как его собирает потребитель библиотеки. */
    private DevicesApi api(String token) {
        RestClient restClient = RestClient.builder()
                .baseUrl("http://localhost:" + port)
                .defaultHeader("Authorization", "Bearer " + token)
                .build();
        HttpServiceProxyFactory factory = HttpServiceProxyFactory.builderFor(RestClientAdapter.create(restClient)).build();
        return factory.createClient(DevicesApi.class);
    }

    private static DeviceCreateRequest createRequest(String id) {
        return new DeviceCreateRequest("t1", "sensor", ManagedStatus.ACTIVE, id)
                .name("sensor-name")
                .ownerId("owner-2")
                .serialNumber("SN-2")
                .manufacturer("ACME")
                .model("T200");
    }

    private static DeviceUpdateRequest updateRequest() {
        return new DeviceUpdateRequest("t1", "sensor", ManagedStatus.ACTIVE)
                .name("sensor-name")
                .ownerId("owner-2")
                .serialNumber("SN-2")
                .manufacturer("ACME")
                .model("T200");
    }

    private static String newId() {
        return "it-client-" + UUID.randomUUID();
    }

    /** Строка шарда напрямую (как запись device-collector-service): карточка есть, так как tenant не null. */
    private void insertDevice(String id, String tenant, long version) {
        jdbc.sql("""
                        INSERT INTO devices (device_id, name, tenant_id, owner_id, device_type, serial_number, manufacturer,
                            model, status, settings, attributes, firmware_version, os_name, os_version, sensors,
                            registered_at, last_seen_at, version, seen_count, first_seen_at, last_event_at,
                            last_event_type, deleted_at)
                        VALUES (:id, 'sensor-name', :tenant, 'owner-1', 'sensor', 'SN-1', 'ACME', 'T100', 'ACTIVE',
                            CAST(:settings AS jsonb), CAST(:attributes AS jsonb), '1.0.0', 'Linux', '6.1',
                            CAST('[]' AS jsonb), now(), now(), :version, 1, now(), now(), 'CREATED', NULL)""")
                .param("id", id)
                .param("tenant", tenant, Types.VARCHAR)
                .param("settings", "{\"mode\":\"eco\"}")
                .param("attributes", "{\"zone\":\"A\"}")
                .param("version", version)
                .update();
    }
}
