package com.github.gluhov.iotplatform.deviceservice.it;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Management-порт device-service (T065): технические точки (health, prometheus) отдаются без токена на отдельном порту,
 * который не публикуется на хост; на основном порту actuator недоступен; гистограммы есть в выводе Prometheus.
 */
class ItManagementTest extends AbstractItTest {

    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static String consumerToken;
    private static String readerToken;

    @BeforeAll
    static void obtainTokens() throws Exception {
        KeycloakTokens keycloak = new KeycloakTokens(keycloakUrl(), KEYCLOAK_ADMIN, KEYCLOAK_ADMIN_PASSWORD);
        consumerToken = keycloak.clientCredentials("device-service-consumer", CONSUMER_SECRET);
        readerToken = keycloak.clientCredentials("device-service-reader", READER_SECRET);
    }

    @Test
    void givenNoToken_whenHealthOnManagementPort_thenUp() throws Exception {
        var response = getManagement("/actuator/health");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("\"status\":\"UP\"");
    }

    @Test
    void givenNoToken_whenLivenessOnManagementPort_thenUp() throws Exception {
        assertThat(getManagement("/actuator/health/liveness").statusCode()).isEqualTo(200);
    }

    @Test
    void givenNoToken_whenPrometheusOnManagementPort_thenMetricsExposed() throws Exception {
        var response = getManagement("/actuator/prometheus");

        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains("jvm_memory_used_bytes");
    }

    @Test
    void givenNoToken_whenPrometheusOnMainPort_thenNotExposed() throws Exception {
        // На основном порту actuator не смонтирован: без токена - 401 (цепочка безопасности), с токеном - 404
        assertThat(send("GET", "/actuator/prometheus", null).statusCode()).isIn(401, 404);
        assertThat(send("GET", "/actuator/prometheus", readerToken).statusCode()).isEqualTo(404);
    }

    @Test
    void givenNoToken_whenApiOnMainPort_thenUnauthorized() throws Exception {
        assertThat(send("GET", "/api/v1/devices/dev-1", null).statusCode()).isEqualTo(401);
    }

    @Test
    void givenRequestAndCommand_whenPrometheus_thenHistogramBucketsPresent() throws Exception {
        String id = "it-mgmt-" + UUID.randomUUID();
        // Один запрос API (ответ не важен: счётчик и гистограмма пишутся по завершении запроса)
        send("GET", "/api/v1/devices/" + id, readerToken);
        // Одна команда: публикация в Kafka измеряется таймером device.command.publish
        var created = HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1/devices"))
                        .timeout(Duration.ofSeconds(30))
                        .header("Authorization", "Bearer " + consumerToken)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(createBody(id)))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(created.statusCode()).isEqualTo(202);

        String metrics = getManagement("/actuator/prometheus").body();

        assertThat(metrics).contains("http_server_requests_seconds_bucket");
        assertThat(metrics).contains("device_command_publish_seconds_bucket");
    }

    private HttpResponse<String> getManagement(String path) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create("http://localhost:" + managementPort + path))
                        .timeout(Duration.ofSeconds(10))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static String createBody(String deviceId) {
        return "{\"deviceId\":\"" + deviceId + "\",\"name\":\"sensor-name\",\"tenantId\":\"t1\",\"ownerId\":\"owner-2\","
                + "\"deviceType\":\"sensor\",\"serialNumber\":\"SN-2\",\"manufacturer\":\"ACME\",\"model\":\"T200\","
                + "\"status\":\"ACTIVE\",\"settings\":{\"mode\":\"eco\"},\"attributes\":{\"zone\":\"B\"}}";
    }
}
