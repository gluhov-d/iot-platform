package com.github.gluhov.iotplatform.deviceservice.it;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.KeyType;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Доступ только для сервисов (US5, FR-006, FR-009, SC-004) - с настоящими токенами Keycloak.
 * Токены: сервисный consumer (чтение и запись), reader (только чтение), сервисный без аудитории, сервисный с истекающим
 * сроком, пользовательский с ролью и аудиторией.
 */
class ItSecurityTest extends AbstractItTest {

    private static final String DEVICE = "/api/v1/devices/dev-1";
    private static final String AUDIENCE = "device-service";
    private static final String USER = "it-user-" + UUID.randomUUID().toString().substring(0, 8);
    private static final String USER_PASSWORD = UUID.randomUUID().toString();
    private static final String NO_AUDIENCE = "it-noaud-" + UUID.randomUUID().toString().substring(0, 8);
    private static final String SHORT_LIVED = "it-short-" + UUID.randomUUID().toString().substring(0, 8);
    private static final String SERVICE_SECRET = UUID.randomUUID().toString();
    private static final String USER_CLIENT = "it-user-client-" + UUID.randomUUID().toString().substring(0, 8);
    private static final String USER_SHORT_CLIENT = "it-user-short-" + UUID.randomUUID().toString().substring(0, 8);
    private static final String USER_NO_AUD_CLIENT = "it-user-noaud-" + UUID.randomUUID().toString().substring(0, 8);
    private static final String USER_NO_ROLE = "it-user-norole-" + UUID.randomUUID().toString().substring(0, 8);
    private static final String FOREIGN_REALM = "it-foreign-" + UUID.randomUUID().toString().substring(0, 8);
    private static final String FOREIGN_CLIENT = "it-foreign-client";

    private static KeycloakTokens keycloak;

    @BeforeAll
    static void provisionClients() throws Exception {
        keycloak = new KeycloakTokens(keycloakUrl(), KEYCLOAK_ADMIN, KEYCLOAK_ADMIN_PASSWORD);
        // Без ролей device-service: при назначенной роли клиента Keycloak сам добавляет его в aud (audience resolve)
        keycloak.createServiceClient(NO_AUDIENCE, SERVICE_SECRET, false, null, List.of());
        keycloak.createServiceClient(SHORT_LIVED, SERVICE_SECRET, true, 1, List.of("devices.read", "devices.write"));
        keycloak.createPublicClient(USER_CLIENT, true);
        keycloak.createPublicClient(USER_SHORT_CLIENT, true, 1);
        keycloak.createPublicClient(USER_NO_AUD_CLIENT, false);
        keycloak.createUser(USER, USER_PASSWORD, List.of("devices.read", "devices.write"));
        // Без ролей device-service: иначе Keycloak добавит device-service в aud (audience resolve)
        keycloak.createUser(USER_NO_ROLE, USER_PASSWORD, List.of());
        keycloak.createForeignRealmServiceClient(FOREIGN_REALM, FOREIGN_CLIENT, SERVICE_SECRET);
    }

    @Test
    void givenNoToken_whenGet_thenUnauthorizedProblemDetails() throws Exception {
        var response = send("GET", DEVICE, null);

        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.headers().firstValue("Content-Type").orElse("")).startsWith("application/problem+json");
        assertThat(response.body()).contains("\"code\":\"UNAUTHORIZED\"", "\"traceId\":\"", "\"status\":401");
        assertThat(response.body()).doesNotContain("Exception", "\"at ");
    }

    @Test
    void givenServiceTokenWithoutAudience_whenGet_thenUnauthorized() throws Exception {
        String token = keycloak.clientCredentials(NO_AUDIENCE, SERVICE_SECRET);

        assertThat(KeycloakTokens.audienceOf(token)).doesNotContain(AUDIENCE);
        assertThat(send("GET", DEVICE, token).statusCode()).isEqualTo(401);
    }

    @Test
    void givenExpiredServiceToken_whenGet_thenUnauthorized() throws Exception {
        String token = keycloak.clientCredentials(SHORT_LIVED, SERVICE_SECRET);
        Thread.sleep(3_000);

        assertThat(send("GET", DEVICE, token).statusCode()).isEqualTo(401);
    }

    @Test
    void givenTokenSignedByOtherKeyWithRealKid_whenGet_thenUnauthorized() throws Exception {
        // Подпись чужим ключом, но с kid настоящего ключа realm: отказ должен идти от проверки подписи, а не от kid
        String token = signedWithOwnKey(keycloak.issuer(), AUDIENCE, realKid());

        var response = send("GET", DEVICE, token);
        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).contains("\"code\":\"UNAUTHORIZED\"").doesNotContain("SERVICE_TOKEN_REQUIRED");
    }

    /** kid первого RSA-ключа realm из JWK Set Keycloak. */
    private static String realKid() throws Exception {
        return JWKSet.parse(keycloak.jwkSetJson()).getKeys().stream()
                .filter(key -> key.getKeyType() == KeyType.RSA)
                .findFirst().orElseThrow().getKeyID();
    }

    /** JWT, подписанный сгенерированным здесь ключом: издатель и аудитория - как у настоящего токена. */
    private static String signedWithOwnKey(String issuer, String audience, String kid) throws Exception {
        var generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair pair = generator.generateKeyPair();
        var header = new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(kid).build();
        var claims = new JWTClaimsSet.Builder()
                .issuer(issuer)
                .audience(audience)
                .subject("it-forged")
                .claim("client_id", "device-service-consumer")
                .expirationTime(Date.from(Instant.now().plusSeconds(300)))
                .build();
        var jwt = new SignedJWT(header, claims);
        jwt.sign(new RSASSASigner(pair.getPrivate()));
        return jwt.serialize();
    }

    @Test
    void givenTokenFromOtherRealm_whenGet_thenUnauthorized() throws Exception {
        String token = keycloak.clientCredentialsInRealm(FOREIGN_REALM, FOREIGN_CLIENT, SERVICE_SECRET);

        assertThat(KeycloakTokens.audienceOf(token)).doesNotContain(AUDIENCE);
        var response = send("GET", DEVICE, token);
        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).contains("\"code\":\"UNAUTHORIZED\"");
    }

    @Test
    void givenExpiredUserToken_whenGet_thenUnauthorizedNotServiceTokenRequired() throws Exception {
        String token = keycloak.userToken(USER_SHORT_CLIENT, USER, USER_PASSWORD);
        Thread.sleep(3_000);

        var response = send("GET", DEVICE, token);

        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).contains("\"code\":\"UNAUTHORIZED\"").doesNotContain("SERVICE_TOKEN_REQUIRED");
    }

    @Test
    void givenUserTokenWithoutAudience_whenGet_thenUnauthorizedNotServiceTokenRequired() throws Exception {
        String token = keycloak.userToken(USER_NO_AUD_CLIENT, USER_NO_ROLE, USER_PASSWORD);

        assertThat(KeycloakTokens.audienceOf(token)).doesNotContain(AUDIENCE);
        var response = send("GET", DEVICE, token);
        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).contains("\"code\":\"UNAUTHORIZED\"").doesNotContain("SERVICE_TOKEN_REQUIRED");
    }

    @Test
    void givenUserTokenWithRoleAndAudience_whenGet_thenServiceTokenRequired() throws Exception {
        String token = keycloak.userToken(USER_CLIENT, USER, USER_PASSWORD);

        var response = send("GET", DEVICE, token);

        assertThat(response.statusCode()).isEqualTo(403);
        assertThat(response.body()).contains("\"code\":\"SERVICE_TOKEN_REQUIRED\"", "\"traceId\":\"");
    }

    @Test
    void givenReaderTokenWithoutWriteRole_whenPut_thenForbiddenProblem() throws Exception {
        String token = keycloak.clientCredentials("device-service-reader", READER_SECRET);

        var response = send("PUT", DEVICE, token);

        assertThat(response.statusCode()).isEqualTo(403);
        assertThat(response.body()).contains("\"code\":\"FORBIDDEN\"", "\"traceId\":\"");
    }

    @Test
    void givenReaderToken_whenGet_thenNotRejectedByAuthorization() throws Exception {
        String token = keycloak.clientCredentials("device-service-reader", READER_SECRET);

        var response = send("GET", DEVICE, token);

        // Доступ разрешён: дальше отвечает чтение (устройства dev-1 нет), а не проверка токена
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.body()).contains("\"code\":\"DEVICE_NOT_FOUND\"");
    }

    @Test
    void givenConsumerToken_whenPut_thenNotRejectedByAuthorization() throws Exception {
        String token = keycloak.clientCredentials("device-service-consumer", CONSUMER_SECRET);

        var response = send("PUT", DEVICE, token);

        // Доступ разрешён: дальше отвечает проверка тела ({} без обязательных полей), а не проверка токена
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("\"code\":\"VALIDATION_FAILED\"");
    }

    @Test
    void givenNoToken_whenHealthLivenessOnMainPort_thenNotOk() throws Exception {
        // Технические точки - только на management-порту (ItManagementTest); основной порт их не открывает
        assertThat(send("GET", "/actuator/health/liveness", null).statusCode()).isIn(401, 404);
    }
}
