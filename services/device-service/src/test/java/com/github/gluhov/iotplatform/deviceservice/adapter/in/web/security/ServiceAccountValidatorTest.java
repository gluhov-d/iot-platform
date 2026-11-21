package com.github.gluhov.iotplatform.deviceservice.adapter.in.web.security;

import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.Jwt;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Сервисный токен Keycloak: есть client_id, нет sid (research.md R8). */
class ServiceAccountValidatorTest {

    private final ServiceAccountValidator validator = new ServiceAccountValidator();

    @Test
    void givenClientIdAndNoSid_whenValidate_thenNoErrors() {
        assertThat(validator.validate(token(Map.of("client_id", "device-service-consumer"))).hasErrors()).isFalse();
    }

    @Test
    void givenSid_whenValidate_thenServiceTokenRequired() {
        var result = validator.validate(token(Map.of("client_id", "web", "sid", "session-1")));
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors().iterator().next().getErrorCode())
                .isEqualTo(ServiceAccountValidator.SERVICE_TOKEN_REQUIRED);
    }

    @Test
    void givenNoClientId_whenValidate_thenServiceTokenRequired() {
        var result = validator.validate(token(Map.of("azp", "web")));
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors().iterator().next().getErrorCode())
                .isEqualTo(ServiceAccountValidator.SERVICE_TOKEN_REQUIRED);
    }

    private static Jwt token(Map<String, Object> extraClaims) {
        var builder = Jwt.withTokenValue("token")
                .header("alg", "RS256")
                .subject("svc")
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(300));
        extraClaims.forEach(builder::claim);
        return builder.build();
    }
}
