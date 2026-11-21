package com.github.gluhov.iotplatform.deviceservice.adapter.in.web.security;

import com.github.gluhov.iotplatform.deviceservice.adapter.in.web.ErrorCode;
import com.github.gluhov.iotplatform.deviceservice.adapter.in.web.ProblemBody;
import com.github.gluhov.iotplatform.deviceservice.adapter.in.web.TraceIdProvider;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.web.access.AccessDeniedHandler;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Сервисный токен без нужной роли (например, reader на PUT) - 403 FORBIDDEN в Problem Details. Пользовательские токены
 * сюда не доходят: их отсекает ProblemAuthenticationEntryPoint с SERVICE_TOKEN_REQUIRED.
 */
@Component
public class ProblemAccessDeniedHandler implements AccessDeniedHandler {

    private final ObjectMapper mapper;
    private final TraceIdProvider traceIds;

    public ProblemAccessDeniedHandler(ObjectMapper mapper, TraceIdProvider traceIds) {
        this.mapper = mapper;
        this.traceIds = traceIds;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response, AccessDeniedException ex)
            throws IOException {
        response.setStatus(ErrorCode.FORBIDDEN.status().value());
        response.setContentType(ProblemBody.PROBLEM_JSON.toString());
        response.setCharacterEncoding("UTF-8");
        response.getWriter().write(mapper.writeValueAsString(ProblemBody.of(ErrorCode.FORBIDDEN,
                ErrorCode.FORBIDDEN.status().value(), ErrorCode.FORBIDDEN.title(),
                "The service token lacks the required role", request.getRequestURI(), traceIds.current(), null)));
    }
}
