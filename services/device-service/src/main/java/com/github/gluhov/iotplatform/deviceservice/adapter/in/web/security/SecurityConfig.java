package com.github.gluhov.iotplatform.deviceservice.adapter.in.web.security;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.client.RestTemplate;

/**
 * Доступ только для сервисов (FR-009, ADR-0007): без состояния, без CSRF (нет сессий и cookie), GET - devices.read,
 * POST/PUT/DELETE - devices.write, технические точки - на management-порту без токена, остальное - с токеном. Проверка токена - decoder с ключами Keycloak
 * (внутренний адрес), iss, сроком, аудиторией и видом учётной записи.
 */
@Configuration
@EnableWebSecurity
@EnableConfigurationProperties(DeviceSecurityProperties.class)
public class SecurityConfig {

    static final String DEVICES_READ = "devices.read";
    static final String DEVICES_WRITE = "devices.write";

    /**
     * Технические точки (/actuator/**) отдаются только на management-порту, который не публикуется на хост:
     * без токена, без CSRF и сессий. На основном порту actuator не смонтирован, и эта цепочка его не открывает.
     */
    @Bean
    @Order(1)
    SecurityFilterChain managementSecurityFilterChain(HttpSecurity http) throws Exception {
        return http
                .securityMatcher("/actuator/**")
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth.anyRequest().permitAll())
                .build();
    }

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http,
                                            ProblemAuthenticationEntryPoint entryPoint,
                                            ProblemAccessDeniedHandler accessDeniedHandler,
                                            JwtDecoder jwtDecoder,
                                            DeviceSecurityProperties properties) throws Exception {
        var jwtConverter = new JwtAuthenticationConverter();
        jwtConverter.setJwtGrantedAuthoritiesConverter(new KeycloakRolesConverter(properties.audience()));
        return http
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .exceptionHandling(ex -> ex.authenticationEntryPoint(entryPoint).accessDeniedHandler(accessDeniedHandler))
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers(HttpMethod.GET, "/api/v1/devices/*").hasAuthority(DEVICES_READ)
                        .requestMatchers(HttpMethod.POST, "/api/v1/devices").hasAuthority(DEVICES_WRITE)
                        .requestMatchers(HttpMethod.PUT, "/api/v1/devices/*").hasAuthority(DEVICES_WRITE)
                        .requestMatchers(HttpMethod.DELETE, "/api/v1/devices/*").hasAuthority(DEVICES_WRITE)
                        .anyRequest().authenticated())
                .oauth2ResourceServer(oauth2 -> oauth2
                        .authenticationEntryPoint(entryPoint)
                        .accessDeniedHandler(accessDeniedHandler)
                        .jwt(jwt -> jwt.decoder(jwtDecoder).jwtAuthenticationConverter(jwtConverter)))
                .build();
    }

    @Bean
    JwtDecoder jwtDecoder(DeviceSecurityProperties properties) {
        // Ключи запрашиваются по внутреннему адресу Keycloak; тайм-ауты - из настроек (не зашиты в код)
        var requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(properties.jwkConnectTimeout());
        requestFactory.setReadTimeout(properties.jwkReadTimeout());
        var restTemplate = new RestTemplate(requestFactory);
        NimbusJwtDecoder decoder = NimbusJwtDecoder.withJwkSetUri(properties.jwkSetUri())
                .restOperations(restTemplate)
                .build();
        decoder.setJwtValidator(tokenValidator(properties));
        return decoder;
    }

    /**
     * Цепочка проверок токена после проверки подписи: срок, iss, аудитория, вид учётной записи.
     * Вынесена отдельно, чтобы проверять её тестом без Keycloak (SecurityConfigIssuerTest).
     */
    static OAuth2TokenValidator<Jwt> tokenValidator(DeviceSecurityProperties properties) {
        return new DelegatingOAuth2TokenValidator<>(
                new JwtTimestampValidator(properties.clockSkew()),
                new JwtIssuerValidator(properties.issuerUri()),
                new AudienceValidator(properties.audience()),
                new ServiceAccountValidator());
    }
}
