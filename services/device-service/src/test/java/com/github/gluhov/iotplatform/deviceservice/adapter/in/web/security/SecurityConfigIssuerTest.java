package com.github.gluhov.iotplatform.deviceservice.adapter.in.web.security;

import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import org.junit.jupiter.api.Test;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.security.oauth2.jwt.JwtValidationException;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Проверка iss изолирована от подписи (FR-009, US5/AC1): токен подписан верным ключом, меняется только iss.
 * Ключ генерируется в тесте, Keycloak не нужен; валидаторы - те же, что в SecurityConfig.
 */
class SecurityConfigIssuerTest {

    private static final String ISSUER = "https://keycloak.test/realms/iot-platform";
    private static final String OTHER_ISSUER = "https://evil.example/realms/other";
    private static final String AUDIENCE = "device-service";

    private final DeviceSecurityProperties properties = new DeviceSecurityProperties(
            ISSUER, "http://unused.test/certs", AUDIENCE, Duration.ofSeconds(30),
            Duration.ofSeconds(1), Duration.ofSeconds(1));

    @Test
    void givenValidSignatureAndExpectedIssuer_whenDecode_thenAccepted() throws Exception {
        var keys = generateKeys();
        var decoder = decoder(keys, SecurityConfig.tokenValidator(properties));

        Jwt jwt = decoder.decode(sign(keys, ISSUER));

        assertThat(jwt.getIssuer().toString()).isEqualTo(ISSUER);
    }

    @Test
    void givenValidSignatureAndForeignIssuer_whenDecode_thenRejectedByIssuerCheck() throws Exception {
        var keys = generateKeys();
        var decoder = decoder(keys, SecurityConfig.tokenValidator(properties));

        assertThatThrownBy(() -> decoder.decode(sign(keys, OTHER_ISSUER)))
                .isInstanceOf(JwtValidationException.class)
                .satisfies(ex -> assertThat(((JwtValidationException) ex).getErrors())
                        .anyMatch(error -> error.getDescription() != null
                                && error.getDescription().contains("iss")));
    }

    private static NimbusJwtDecoder decoder(KeyPair keys, OAuth2TokenValidator<Jwt> validator) {
        var decoder = NimbusJwtDecoder.withPublicKey((RSAPublicKey) keys.getPublic()).build();
        decoder.setJwtValidator(validator);
        return decoder;
    }

    private static String sign(KeyPair keys, String issuer) {
        var rsaKey = new RSAKey.Builder((RSAPublicKey) keys.getPublic())
                .privateKey((RSAPrivateKey) keys.getPrivate())
                .keyID("test-key")
                .build();
        var encoder = new NimbusJwtEncoder(new ImmutableJWKSet<>(new JWKSet(rsaKey)));
        var now = Instant.now();
        var claims = JwtClaimsSet.builder()
                .issuer(issuer)
                .audience(List.of(AUDIENCE))
                .subject("device-collector")
                .issuedAt(now)
                .expiresAt(now.plusSeconds(300))
                .claim("client_id", "device-collector")
                .build();
        var header = JwsHeader.with(org.springframework.security.oauth2.jose.jws.SignatureAlgorithm.RS256)
                .keyId("test-key").build();
        return encoder.encode(JwtEncoderParameters.from(header, claims)).getTokenValue();
    }

    private static KeyPair generateKeys() throws Exception {
        var generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        return generator.generateKeyPair();
    }
}
