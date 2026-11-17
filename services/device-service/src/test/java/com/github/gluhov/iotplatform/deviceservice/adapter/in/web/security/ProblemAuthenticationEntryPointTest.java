package com.github.gluhov.iotplatform.deviceservice.adapter.in.web.security;

import com.github.gluhov.iotplatform.deviceservice.adapter.in.web.TraceIdProvider;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2ErrorCodes;
import org.springframework.security.oauth2.jwt.JwtValidationException;
import org.springframework.security.oauth2.server.resource.InvalidBearerTokenException;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ProblemAuthenticationEntryPointTest {

    private final ProblemAuthenticationEntryPoint entryPoint = new ProblemAuthenticationEntryPoint(
            new ObjectMapper(),
            new TraceIdProvider(new StaticListableBeanFactory().getBeanProvider(io.micrometer.tracing.Tracer.class)));

    @Test
    void givenInvalidToken_whenCommence_thenUnauthorizedProblem() throws Exception {
        var response = new MockHttpServletResponse();

        entryPoint.commence(new MockHttpServletRequest("GET", "/api/v1/devices/dev-1"), response,
                new InvalidBearerTokenException("The token is expired"));

        assertThat(response.getStatus()).isEqualTo(401);
        assertThat(response.getContentType()).startsWith("application/problem+json");
        assertThat(response.getContentAsString()).contains("\"code\":\"UNAUTHORIZED\"", "\"traceId\":\"");
    }

    @Test
    void givenUserTokenCause_whenCommence_thenServiceTokenRequiredProblem() throws Exception {
        var response = new MockHttpServletResponse();
        var cause = new JwtValidationException("Service token required", List.of(
                new OAuth2Error(ServiceAccountValidator.SERVICE_TOKEN_REQUIRED, "user token", null)));

        entryPoint.commence(new MockHttpServletRequest("PUT", "/api/v1/devices/dev-1"), response,
                new InvalidBearerTokenException(cause.getMessage(), cause));

        assertThat(response.getStatus()).isEqualTo(403);
        assertThat(response.getContentAsString()).contains("\"code\":\"SERVICE_TOKEN_REQUIRED\"");
    }

    @Test
    void givenOtherValidationError_whenCommence_thenUnauthorized() throws Exception {
        var response = new MockHttpServletResponse();
        var cause = new JwtValidationException("bad", List.of(new OAuth2Error(OAuth2ErrorCodes.INVALID_TOKEN)));

        entryPoint.commence(new MockHttpServletRequest("GET", "/api/v1/devices/dev-1"), response,
                new InvalidBearerTokenException(cause.getMessage(), cause));

        assertThat(response.getStatus()).isEqualTo(401);
    }

    @Test
    void givenUserTokenAlsoExpired_whenCommence_thenUnauthorizedNotServiceTokenRequired() throws Exception {
        var response = new MockHttpServletResponse();
        var cause = new JwtValidationException("expired", List.of(
                new OAuth2Error(ServiceAccountValidator.SERVICE_TOKEN_REQUIRED, "user token", null),
                new OAuth2Error(OAuth2ErrorCodes.INVALID_TOKEN, "The Jwt expired", null)));

        entryPoint.commence(new MockHttpServletRequest("PUT", "/api/v1/devices/dev-1"), response,
                new InvalidBearerTokenException(cause.getMessage(), cause));

        assertThat(response.getStatus()).isEqualTo(401);
        assertThat(response.getContentAsString()).contains("\"code\":\"UNAUTHORIZED\"")
                .doesNotContain("SERVICE_TOKEN_REQUIRED");
    }

    @Test
    void givenUserTokenWithoutAudience_whenCommence_thenUnauthorized() throws Exception {
        var response = new MockHttpServletResponse();
        var cause = new JwtValidationException("aud", List.of(
                new OAuth2Error(ServiceAccountValidator.SERVICE_TOKEN_REQUIRED, "user token", null),
                new OAuth2Error(OAuth2ErrorCodes.INVALID_TOKEN, "The aud claim is not valid", null)));

        entryPoint.commence(new MockHttpServletRequest("GET", "/api/v1/devices/dev-1"), response,
                new InvalidBearerTokenException(cause.getMessage(), cause));

        assertThat(response.getStatus()).isEqualTo(401);
    }
}
