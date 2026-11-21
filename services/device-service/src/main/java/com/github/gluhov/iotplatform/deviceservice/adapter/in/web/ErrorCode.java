package com.github.gluhov.iotplatform.deviceservice.adapter.in.web;

import org.springframework.http.HttpStatus;

/**
 * Коды ошибок API - ровно значения {@code Problem.code} контракта device-service.yaml (проверяет ErrorCodeTest).
 * Статус и заголовок - по таблице ошибок plan.md; тип ошибки - URI по имени кода в kebab-case.
 */
public enum ErrorCode {

    VALIDATION_FAILED(HttpStatus.BAD_REQUEST, "Validation failed"),
    UNAUTHORIZED(HttpStatus.UNAUTHORIZED, "Unauthorized"),
    SERVICE_TOKEN_REQUIRED(HttpStatus.FORBIDDEN, "Service token required"),
    FORBIDDEN(HttpStatus.FORBIDDEN, "Forbidden"),
    DEVICE_NOT_FOUND(HttpStatus.NOT_FOUND, "Device not found"),
    DEVICE_ALREADY_EXISTS(HttpStatus.CONFLICT, "Device already exists"),
    COMMAND_NOT_ACCEPTED(HttpStatus.SERVICE_UNAVAILABLE, "Command not accepted"),
    READ_MODEL_UNAVAILABLE(HttpStatus.SERVICE_UNAVAILABLE, "Read model unavailable"),
    INTERNAL_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "Internal error");

    private static final String TYPE_BASE = "https://iot-platform.example/problems/";

    private final HttpStatus status;
    private final String title;

    ErrorCode(HttpStatus status, String title) {
        this.status = status;
        this.title = title;
    }

    public HttpStatus status() {
        return status;
    }

    public String title() {
        return title;
    }

    /** Тип ошибки (RFC 9457): база и имя кода в kebab-case, например device-not-found. */
    public String type() {
        return TYPE_BASE + name().toLowerCase(java.util.Locale.ROOT).replace('_', '-');
    }
}
