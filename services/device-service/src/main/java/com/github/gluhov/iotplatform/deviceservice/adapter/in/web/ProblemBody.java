package com.github.gluhov.iotplatform.deviceservice.adapter.in.web;

import org.springframework.http.MediaType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Тело ошибки Problem Details (RFC 9457) с полями контракта: type, title, status, code, traceId и необязательные
 * detail, instance, errors. Стек, тексты исключений и SQL сюда не попадают - их видит только лог.
 * Общее для обработчика MVC и фильтров безопасности (401/403).
 */
public final class ProblemBody {

    public static final MediaType PROBLEM_JSON = MediaType.parseMediaType("application/problem+json");

    private ProblemBody() {
    }

    public static Map<String, Object> of(ErrorCode code, int status, String title, String detail, String instance,
                                         String traceId, List<Map<String, String>> errors) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("type", code.type());
        body.put("title", title);
        body.put("status", status);
        if (detail != null) {
            body.put("detail", detail);
        }
        if (instance != null) {
            body.put("instance", instance);
        }
        body.put("code", code.name());
        body.put("traceId", traceId);
        if (errors != null) {
            body.put("errors", errors);
        }
        return body;
    }
}
