package com.github.gluhov.iotplatform.deviceservice.adapter.in.web.security;

import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.Jwt;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AudienceValidatorTest {

    private final AudienceValidator validator = new AudienceValidator("device-service");

    @Test
    void givenAudienceContainsDeviceService_whenValidate_thenNoErrors() {
        assertThat(validator.validate(token(List.of("account", "device-service"))).hasErrors()).isFalse();
    }

    @Test
    void givenNoAudience_whenValidate_thenInvalidToken() {
        var result = validator.validate(token(null));
        assertThat(result.hasErrors()).isTrue();
        assertThat(result.getErrors().iterator().next().getErrorCode()).isEqualTo("invalid_token");
    }

    @Test
    void givenOtherAudienceOnly_whenValidate_thenInvalidToken() {
        assertThat(validator.validate(token(List.of("account"))).hasErrors()).isTrue();
    }

    private static Jwt token(List<String> audience) {
        var builder = Jwt.withTokenValue("token")
                .header("alg", "RS256")
                .subject("svc")
                .issuedAt(Instant.now())
                .expiresAt(Instant.now().plusSeconds(300));
        if (audience != null) {
            builder.audience(audience);
        }
        return builder.build();
    }
}
