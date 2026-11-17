package com.github.gluhov.iotplatform.deviceservice.it;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Помощник токенов для интеграционных тестов: токены получаются у настоящего Keycloak. Клиенты и пользователи, которых
 * нет в realm из репозитория, создаются через Admin API (ADR-0007). Пароли и секреты передаёт вызывающий код
 * (в тестах - случайные значения, в коде их нет).
 */
final class KeycloakTokens {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private static final String REALM = "iot-platform";
    private static final String SERVICE_CLIENT = "device-service";
    private static final String AUDIENCE = "device-service";

    private final String base;
    private final String adminUser;
    private final String adminPassword;
    private String adminToken;

    KeycloakTokens(String base, String adminUser, String adminPassword) {
        this.base = base;
        this.adminUser = adminUser;
        this.adminPassword = adminPassword;
    }

    /** Токен сервисной учётной записи клиента (client credentials). */
    String clientCredentials(String clientId, String secret) throws Exception {
        return tokenAt(realmUrl("/protocol/openid-connect/token"), Map.of(
                "grant_type", "client_credentials", "client_id", clientId, "client_secret", secret));
    }

    /** Токен пользователя (password grant) через публичный клиент с прямым входом. */
    String userToken(String clientId, String username, String password) throws Exception {
        return tokenAt(realmUrl("/protocol/openid-connect/token"), Map.of(
                "grant_type", "password", "client_id", clientId, "username", username, "password", password));
    }

    /** Публичный клиент для пользовательского токена; с аудиторией device-service или без неё. */
    void createPublicClient(String clientId, boolean withAudience) throws Exception {
        createPublicClient(clientId, withAudience, null);
    }

    /** То же, со сроком токена (секунды) - атрибут клиента, если задан. */
    void createPublicClient(String clientId, boolean withAudience, Integer lifespanSeconds) throws Exception {
        ObjectNode client = baseClient(clientId);
        client.put("publicClient", true);
        client.put("directAccessGrantsEnabled", true);
        client.put("serviceAccountsEnabled", false);
        if (withAudience) {
            addAudienceMapper(client);
        }
        if (lifespanSeconds != null) {
            client.putObject("attributes").put("access.token.lifespan", String.valueOf(lifespanSeconds));
        }
        adminPost("/clients", client);
    }

    /**
     * Отдельный realm с сервисным клиентом: токен того же вида, но с другим издателем (iss). Realm создаётся через
     * Admin API мастер-realm; имя - уникальное на запуск.
     */
    void createForeignRealmServiceClient(String realm, String clientId, String secret) throws Exception {
        ObjectNode body = JSON.createObjectNode();
        body.put("realm", realm);
        body.put("enabled", true);
        adminPostAt("/realms", body);
        ObjectNode client = baseClient(clientId);
        client.put("publicClient", false);
        client.put("serviceAccountsEnabled", true);
        client.put("secret", secret);
        adminPostAt("/realms/" + enc(realm) + "/clients", client);
    }

    /** Токен сервисной учётной записи из другого realm (другой iss). */
    String clientCredentialsInRealm(String realm, String clientId, String secret) throws Exception {
        return tokenAt(base + "/realms/" + realm + "/protocol/openid-connect/token", Map.of(
                "grant_type", "client_credentials", "client_id", clientId, "client_secret", secret));
    }

    /** Набор ключей подписи realm (JWK Set JSON) - нужен тесту, чтобы подписать токен чужим ключом с известным kid. */
    String jwkSetJson() throws Exception {
        HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(URI.create(
                        realmUrl("/protocol/openid-connect/certs")))
                .timeout(Duration.ofSeconds(20)).GET().build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Keycloak не отдал ключи realm: HTTP " + response.statusCode());
        }
        return response.body();
    }

    /**
     * Конфиденциальный клиент сервисной учётной записи. Срок токена (секунды) - атрибут клиента, если задан.
     * Роли device-service назначаются его сервисной учётной записи.
     */
    void createServiceClient(String clientId, String secret, boolean withAudience, Integer lifespanSeconds,
                             List<String> roles) throws Exception {
        ObjectNode client = baseClient(clientId);
        client.put("publicClient", false);
        client.put("serviceAccountsEnabled", true);
        client.put("secret", secret);
        if (withAudience) {
            addAudienceMapper(client);
        }
        if (lifespanSeconds != null) {
            client.putObject("attributes").put("access.token.lifespan", String.valueOf(lifespanSeconds));
        }
        adminPost("/clients", client);
        assignRoles("/users/" + userId("service-account-" + clientId), roles);
    }

    /** Пользователь realm с ролями device-service (для пользовательского токена). */
    void createUser(String username, String password, List<String> roles) throws Exception {
        ObjectNode user = JSON.createObjectNode();
        user.put("username", username);
        user.put("enabled", true);
        user.put("emailVerified", true);
        user.put("email", username + "@it.example");
        user.put("firstName", "Integration");
        user.put("lastName", "Test");
        user.putArray("credentials").addObject()
                .put("type", "password").put("value", password).put("temporary", false);
        adminPost("/users", user);
        assignRoles("/users/" + userId(username), roles);
    }

    String issuer() {
        return realmUrl("");
    }

    /** Значение aud из payload токена (без проверки подписи - только для утверждения о составе токена в тесте). */
    static java.util.List<String> audienceOf(String token) {
        String payload = token.split("\\.")[1];
        JsonNode aud = JSON.readTree(java.util.Base64.getUrlDecoder().decode(payload)).get("aud");
        java.util.List<String> result = new java.util.ArrayList<>();
        if (aud != null && aud.isArray()) {
            aud.forEach(a -> result.add(a.asString()));
        } else if (aud != null) {
            result.add(aud.asString());
        }
        return result;
    }

    private String realmUrl(String suffix) {
        return base + "/realms/" + REALM + suffix;
    }

    private ObjectNode baseClient(String clientId) {
        ObjectNode client = JSON.createObjectNode();
        client.put("clientId", clientId);
        client.put("enabled", true);
        return client;
    }

    private static void addAudienceMapper(ObjectNode client) {
        client.putArray("protocolMappers").addObject()
                .put("name", "device-service-audience")
                .put("protocol", "openid-connect")
                .put("protocolMapper", "oidc-audience-mapper")
                .put("consentRequired", false)
                .set("config", JSON.createObjectNode()
                        .put("included.client.audience", AUDIENCE)
                        .put("id.token.claim", "false")
                        .put("access.token.claim", "true"));
    }

    private void assignRoles(String userPath, List<String> roles) throws Exception {
        String clientUuid = adminGet("/clients?clientId=" + enc(SERVICE_CLIENT)).get(0).get("id").asString();
        ArrayNode mapping = JSON.createArrayNode();
        for (String role : roles) {
            mapping.add(adminGet("/clients/" + clientUuid + "/roles/" + enc(role)));
        }
        adminPost(userPath + "/role-mappings/clients/" + clientUuid, mapping);
    }

    private String userId(String username) throws Exception {
        return adminGet("/users?username=" + enc(username) + "&exact=true").get(0).get("id").asString();
    }

    private JsonNode adminGet(String path) throws Exception {
        HttpResponse<String> response = HTTP.send(adminRequest(path).GET().build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Admin API GET " + path + ": HTTP " + response.statusCode());
        }
        return JSON.readTree(response.body());
    }

    /** Создание и назначение ролей: 409 при создании (уже есть) не ошибка - тесты могут повторяться. */
    private void adminPost(String path, JsonNode body) throws Exception {
        adminPostAt("/realms/" + REALM + path, body);
    }

    /** Путь - от /admin (например, /realms для создания realm). */
    private void adminPostAt(String adminPath, JsonNode body) throws Exception {
        HttpResponse<String> response = HTTP.send(adminRequestAt(adminPath)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)))
                .build(), HttpResponse.BodyHandlers.ofString());
        int status = response.statusCode();
        if (status != 409 && status / 100 != 2) {
            throw new IllegalStateException("Admin API POST " + adminPath + ": HTTP " + status + " " + response.body());
        }
    }

    private HttpRequest.Builder adminRequest(String path) throws Exception {
        return adminRequestAt("/realms/" + REALM + path);
    }

    private HttpRequest.Builder adminRequestAt(String adminPath) throws Exception {
        return HttpRequest.newBuilder(URI.create(base + "/admin" + adminPath))
                .timeout(Duration.ofSeconds(20))
                .header("Authorization", "Bearer " + adminToken());
    }

    private String adminToken() throws Exception {
        if (adminToken == null) {
            adminToken = tokenAt(base + "/realms/master/protocol/openid-connect/token", Map.of(
                    "grant_type", "password", "client_id", "admin-cli",
                    "username", adminUser, "password", adminPassword));
        }
        return adminToken;
    }

    private static String tokenAt(String url, Map<String, String> form) throws Exception {
        StringBuilder body = new StringBuilder();
        form.forEach((k, v) -> {
            if (!body.isEmpty()) {
                body.append('&');
            }
            body.append(enc(k)).append('=').append(enc(v));
        });
        HttpResponse<String> response = HTTP.send(HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Keycloak отказал в токене: HTTP " + response.statusCode());
        }
        return JSON.readTree(response.body()).get("access_token").asString();
    }

    private static String enc(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
