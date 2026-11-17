package com.github.gluhov.iotplatform.deviceservice.adapter.in.web.security;

import org.junit.jupiter.api.Test;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class KeycloakRolesConverterTest {

    private final KeycloakRolesConverter converter = new KeycloakRolesConverter("device-service");

    @Test
    void givenRolesOfDeviceService_whenConvert_thenAuthoritiesWithoutPrefix() {
        Jwt jwt = token(Map.of("resource_access", Map.of(
                "device-service", Map.of("roles", java.util.List.of("devices.read", "devices.write")))));

        assertThat(authorities(jwt)).containsExactlyInAnyOrder("devices.read", "devices.write");
    }

    @Test
    void givenRolesOfOtherClientOnly_whenConvert_thenNoAuthorities() {
        Jwt jwt = token(Map.of("resource_access", Map.of(
                "account", Map.of("roles", java.util.List.of("manage-account")))));

        assertThat(authorities(jwt)).isEmpty();
    }

    @Test
    void givenNoResourceAccess_whenConvert_thenNoAuthorities() {
        assertThat(authorities(token(Map.of()))).isEmpty();
    }

    private Set<String> authorities(Jwt jwt) {
        return converter.convert(jwt).stream()
                .map(GrantedAuthority::getAuthority)
                .collect(Collectors.toSet());
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
