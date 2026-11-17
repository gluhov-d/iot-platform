package com.github.gluhov.iotplatform.deviceservice.adapter.in.web.security;

import com.github.gluhov.iotplatform.deviceservice.adapter.in.web.ErrorCode;
import com.github.gluhov.iotplatform.deviceservice.adapter.in.web.ProblemBody;
import com.github.gluhov.iotplatform.deviceservice.adapter.in.web.TraceIdProvider;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.jwt.JwtValidationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Нет токена, подпись, срок, aud или iss не прошли - 401 UNAUTHORIZED. Пользовательский токен (отклонён
 * ServiceAccountValidator) - 403 SERVICE_TOKEN_REQUIRED. Тело - Problem Details без причин отказа.
 */
@Component
public class ProblemAuthenticationEntryPoint implements AuthenticationEntryPoint {

    private final ObjectMapper mapper;
    private final TraceIdProvider traceIds;

    public ProblemAuthenticationEntryPoint(ObjectMapper mapper, TraceIdProvider traceIds) {
        this.mapper = mapper;
        this.traceIds = traceIds;
    }

    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException ex)
            throws IOException {
        ErrorCode code = isServiceTokenRequired(ex) ? ErrorCode.SERVICE_TOKEN_REQUIRED : ErrorCode.UNAUTHORIZED;
        if (code == ErrorCode.UNAUTHORIZED) {
            response.setHeader("WWW-Authenticate", "Bearer");
        }
        String detail = code == ErrorCode.SERVICE_TOKEN_REQUIRED
                ? "Only service account tokens are accepted"
                : "Missing or invalid bearer token";
        write(response, code, detail, request);
    }

    private void write(HttpServletResponse response, ErrorCode code, String detail, HttpServletRequest request)
            throws IOException {
        response.setStatus(code.status().value());
        response.setContentType(ProblemBody.PROBLEM_JSON.toString());
        response.setCharacterEncoding("UTF-8");
        response.getWriter().write(mapper.writeValueAsString(ProblemBody.of(code, code.status().value(),
                code.title(), detail, request.getRequestURI(), traceIds.current(), null)));
    }

    /**
     * 403 только если единственная ошибка проверки - отсутствие признаков сервисной учётной записи. Любая другая
     * ошибка (срок, iss, aud, подпись) при этом же токене - 401: токен не проверен до конца, его вид не раскрываем.
     */
    static boolean isServiceTokenRequired(Throwable ex) {
        if (ex.getCause() instanceof JwtValidationException validation) {
            var errors = validation.getErrors();
            return errors.size() == 1
                    && ServiceAccountValidator.SERVICE_TOKEN_REQUIRED.equals(errors.iterator().next().getErrorCode());
        }
        return false;
    }
}
