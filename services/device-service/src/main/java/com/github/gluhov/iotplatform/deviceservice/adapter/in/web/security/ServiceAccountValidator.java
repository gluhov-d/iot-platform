package com.github.gluhov.iotplatform.deviceservice.adapter.in.web.security;

import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;

/**
 * Токен получен сервисной учётной записью (client credentials): есть client_id и нет sid (research.md R8).
 * Пользовательский токен отклоняется с кодом {@link #SERVICE_TOKEN_REQUIRED}; ответ на него - 403 (ProblemAuthenticationEntryPoint).
 */
public class ServiceAccountValidator implements OAuth2TokenValidator<Jwt> {

    public static final String SERVICE_TOKEN_REQUIRED = "service_token_required";

    @Override
    public OAuth2TokenValidatorResult validate(Jwt token) {
        if (token.hasClaim("client_id") && !token.hasClaim("sid")) {
            return OAuth2TokenValidatorResult.success();
        }
        return OAuth2TokenValidatorResult.failure(new OAuth2Error(SERVICE_TOKEN_REQUIRED,
                "Only service account tokens are accepted", null));
    }
}
