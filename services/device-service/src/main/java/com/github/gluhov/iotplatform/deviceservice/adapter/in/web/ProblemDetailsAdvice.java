package com.github.gluhov.iotplatform.deviceservice.adapter.in.web;

import com.github.gluhov.iotplatform.deviceservice.domain.exception.CommandNotAcceptedException;
import com.github.gluhov.iotplatform.deviceservice.domain.exception.DeviceAlreadyExistsException;
import com.github.gluhov.iotplatform.deviceservice.domain.exception.DeviceNotFoundException;
import com.github.gluhov.iotplatform.deviceservice.domain.exception.ReadModelUnavailableException;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.ErrorResponse;
import org.springframework.web.HttpMediaTypeNotAcceptableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.ErrorResponseException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.util.List;
import java.util.Map;

/**
 * Ошибки MVC в Problem Details (FR-006): проверка тела и параметров - 400 VALIDATION_FAILED с errors[],
 * ошибки Spring с HTTP-статусом - тот же статус, неожиданное исключение - 500 INTERNAL_ERROR без подробностей
 * (полный стек - в лог, с traceId для связи).
 */
@RestControllerAdvice
public class ProblemDetailsAdvice {

    private static final Logger log = LoggerFactory.getLogger(ProblemDetailsAdvice.class);

    private final TraceIdProvider traceIds;

    public ProblemDetailsAdvice(TraceIdProvider traceIds) {
        this.traceIds = traceIds;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<Map<String, Object>> handleBodyValidation(MethodArgumentNotValidException ex, HttpServletRequest request) {
        List<Map<String, String>> errors = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> Map.of("field", fe.getField(),
                        "message", fe.getDefaultMessage() == null ? "invalid" : fe.getDefaultMessage()))
                .toList();
        return problem(ErrorCode.VALIDATION_FAILED, HttpStatus.BAD_REQUEST.value(), "Request body is invalid",
                request, errors);
    }

    @ExceptionHandler(HandlerMethodValidationException.class)
    ResponseEntity<Map<String, Object>> handleParameterValidation(HandlerMethodValidationException ex,
                                                                  HttpServletRequest request) {
        List<Map<String, String>> errors = ex.getParameterValidationResults().stream()
                .flatMap(result -> result.getResolvableErrors().stream()
                        .map(error -> Map.of("field", String.valueOf(result.getMethodParameter().getParameterName()),
                                "message", error.getDefaultMessage() == null ? "invalid" : error.getDefaultMessage())))
                .toList();
        return problem(ErrorCode.VALIDATION_FAILED, HttpStatus.BAD_REQUEST.value(), "Request parameters are invalid",
                request, errors);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<Map<String, Object>> handleUnreadableBody(HttpMessageNotReadableException ex, HttpServletRequest request) {
        return problem(ErrorCode.VALIDATION_FAILED, HttpStatus.BAD_REQUEST.value(), "Request body is not readable",
                request, null);
    }

    /**
     * Исключения Spring с HTTP-статусом (в том числе ResponseStatusException): статус сохраняется. Код - VALIDATION_FAILED:
     * контракт не даёт отдельного кода для прочих 4xx (405, 406, 415, неизвестный маршрут), README 22.8.
     */
    @ExceptionHandler(ErrorResponseException.class)
    ResponseEntity<Map<String, Object>> handleErrorResponse(ErrorResponseException ex, HttpServletRequest request) {
        HttpStatusCode statusCode = ex.getStatusCode();
        if (statusCode.is5xxServerError()) {
            return unexpected(ex, request);
        }
        int status = statusCode.value();
        ErrorCode code = ErrorCode.VALIDATION_FAILED;
        String detail = ex.getBody().getDetail();
        return ResponseEntity.status(statusCode)
                .contentType(ProblemBody.PROBLEM_JSON)
                .body(ProblemBody.of(code, status, ex.getBody().getTitle() != null
                                ? ex.getBody().getTitle() : code.title(),
                        detail, request.getRequestURI(), traceIds.current(), null));
    }

    /**
     * Неизвестный путь (в том числе /actuator/** на основном порту, где actuator не смонтирован): 404 Problem Details,
     * код - VALIDATION_FAILED, как для прочих 4xx выше. Без этого обработчика Spring отдаёт 500.
     */
    @ExceptionHandler(NoResourceFoundException.class)
    ResponseEntity<Map<String, Object>> handleNoResource(NoResourceFoundException ex, HttpServletRequest request) {
        return problem(ErrorCode.VALIDATION_FAILED, HttpStatus.NOT_FOUND.value(), "Resource not found", request, null);
    }

    /**
     * 405, 415, 406 от Spring: исключения сами несут статус (ErrorResponse), обработчик Exception.class их бы
     * превратил в 500. Статус и заголовки (Allow у 405) сохраняются; код - VALIDATION_FAILED (README 22.8).
     */
    @ExceptionHandler({HttpRequestMethodNotSupportedException.class, HttpMediaTypeNotSupportedException.class,
            HttpMediaTypeNotAcceptableException.class})
    ResponseEntity<Map<String, Object>> handleMethodOrMediaType(ErrorResponse ex, HttpServletRequest request) {
        HttpStatusCode statusCode = ex.getStatusCode();
        int status = statusCode.value();
        String title = ex.getBody().getTitle() != null ? ex.getBody().getTitle() : ErrorCode.VALIDATION_FAILED.title();
        return ResponseEntity.status(statusCode)
                .headers(ex.getHeaders())
                .contentType(ProblemBody.PROBLEM_JSON)
                .body(ProblemBody.of(ErrorCode.VALIDATION_FAILED, status, title, ex.getBody().getDetail(),
                        request.getRequestURI(), traceIds.current(), null));
    }

    @ExceptionHandler(DeviceNotFoundException.class)
    ResponseEntity<Map<String, Object>> handleDeviceNotFound(DeviceNotFoundException ex, HttpServletRequest request) {
        return problem(ErrorCode.DEVICE_NOT_FOUND, ErrorCode.DEVICE_NOT_FOUND.status().value(),
                "Device " + ex.getDeviceId() + " not found", request, null);
    }

    @ExceptionHandler(DeviceAlreadyExistsException.class)
    ResponseEntity<Map<String, Object>> handleDeviceAlreadyExists(DeviceAlreadyExistsException ex,
                                                                  HttpServletRequest request) {
        return problem(ErrorCode.DEVICE_ALREADY_EXISTS, ErrorCode.DEVICE_ALREADY_EXISTS.status().value(),
                "Device " + ex.getDeviceId() + " already exists", request, null);
    }

    /** Kafka не подтвердила команду: ответ без подробностей, причина - в логе под тем же traceId. */
    @ExceptionHandler(CommandNotAcceptedException.class)
    ResponseEntity<Map<String, Object>> handleCommandNotAccepted(CommandNotAcceptedException ex,
                                                                 HttpServletRequest request) {
        log.error("Команда не подтверждена Kafka, deviceId={}, traceId={}, путь={}", ex.getDeviceId(),
                traceIds.current(), request.getRequestURI(), ex);
        return problem(ErrorCode.COMMAND_NOT_ACCEPTED, ErrorCode.COMMAND_NOT_ACCEPTED.status().value(),
                "Command not accepted, see traceId", request, null);
    }

    /** Модель чтения недоступна: ответ без подробностей, причина - в логе под тем же traceId. */
    @ExceptionHandler(ReadModelUnavailableException.class)
    ResponseEntity<Map<String, Object>> handleReadModelUnavailable(ReadModelUnavailableException ex,
                                                                   HttpServletRequest request) {
        log.error("Модель чтения недоступна, traceId={}, путь={}", traceIds.current(), request.getRequestURI(), ex);
        return problem(ErrorCode.READ_MODEL_UNAVAILABLE, ErrorCode.READ_MODEL_UNAVAILABLE.status().value(),
                "Read model is unavailable, see traceId", request, null);
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<Map<String, Object>> handleUnexpected(Exception ex, HttpServletRequest request) {
        return unexpected(ex, request);
    }

    private ResponseEntity<Map<String, Object>> unexpected(Exception ex, HttpServletRequest request) {
        String traceId = traceIds.current();
        log.error("Необработанная ошибка, traceId={}, путь={}", traceId, request.getRequestURI(), ex);
        return ResponseEntity.status(ErrorCode.INTERNAL_ERROR.status())
                .contentType(ProblemBody.PROBLEM_JSON)
                .body(ProblemBody.of(ErrorCode.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR.status().value(),
                        ErrorCode.INTERNAL_ERROR.title(), "Internal error, see traceId", request.getRequestURI(),
                        traceId, null));
    }

    private ResponseEntity<Map<String, Object>> problem(ErrorCode code, int status, String detail,
                                                        HttpServletRequest request, List<Map<String, String>> errors) {
        return ResponseEntity.status(status)
                .contentType(ProblemBody.PROBLEM_JSON)
                .body(ProblemBody.of(code, status, code.title(), detail, request.getRequestURI(),
                        traceIds.current(), errors));
    }
}
