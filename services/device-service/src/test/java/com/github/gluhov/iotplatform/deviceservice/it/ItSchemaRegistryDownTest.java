package com.github.gluhov.iotplatform.deviceservice.it;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.DockerClientFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Schema Registry не отвечает (FR-002): ответ 503 COMMAND_NOT_ACCEPTED не позже срока отправки команды. Сериализатор
 * Avro запрашивает id схемы у Schema Registry при первой отправке и дальше берёт его из кэша, поэтому тесту нужен свой
 * контекст Spring (свойство ниже) - новый продюсер с пустым кэшем; Schema Registry ставится на паузу до первой отправки.
 */
@TestPropertySource(properties = "app.it.context=schema-registry-down")
class ItSchemaRegistryDownTest extends AbstractItTest {

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static String consumerToken;

    @BeforeAll
    static void obtainConsumerToken() throws Exception {
        consumerToken = new KeycloakTokens(keycloakUrl(), KEYCLOAK_ADMIN, KEYCLOAK_ADMIN_PASSWORD)
                .clientCredentials("device-service-consumer", CONSUMER_SECRET);
    }

    @Test
    void givenSchemaRegistryNotResponding_whenPost_thenCommandNotAcceptedInTime() throws Exception {
        String id = "it-sr-" + UUID.randomUUID();
        String body = "{\"deviceId\":\"" + id + "\",\"tenantId\":\"t1\",\"deviceType\":\"sensor\",\"status\":\"NEW\"}";
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1/devices"))
                .timeout(Duration.ofSeconds(20))
                .header("Authorization", "Bearer " + consumerToken)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        String registryId = SCHEMA_REGISTRY.getContainerId();
        HttpResponse<String> response = null;
        long started = System.nanoTime();
        // Пауза, а не остановка: порт не меняется. Снимается в finally, чтобы не задеть другие классы
        DockerClientFactory.instance().client().pauseContainerCmd(registryId).exec();
        try {
            response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (HttpTimeoutException e) {
            fail("нет ответа за 20 с при неотвечающем Schema Registry");
        } finally {
            DockerClientFactory.instance().client().unpauseContainerCmd(registryId).exec();
        }
        Duration elapsed = Duration.ofNanos(System.nanoTime() - started);

        assertThat(response.statusCode()).isEqualTo(503);
        assertThat(response.body()).contains("\"code\":\"COMMAND_NOT_ACCEPTED\"");
        // Срок отправки в тестовом профиле - 2 с; запас на HTTP, как в ItDeviceCommandsTest (Kafka недоступна)
        assertThat(elapsed).isLessThan(Duration.ofSeconds(6));
    }
}
