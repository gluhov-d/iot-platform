package com.github.gluhov.iotplatform.deviceservice.adapter.in.web;

import jakarta.validation.Valid;
import com.github.gluhov.iotplatform.deviceservice.domain.exception.DeviceNotFoundException;
import com.github.gluhov.iotplatform.deviceservice.domain.exception.ReadModelUnavailableException;
import jakarta.validation.constraints.NotBlank;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Ошибки MVC - Problem Details с code и traceId; внутренности (тексты исключений, SQL, стек) в ответе не попадают.
 */
class ProblemDetailsAdviceTest {

    private final MockMvc mockMvc = MockMvcBuilders.standaloneSetup(new FailingController())
            .setControllerAdvice(new ProblemDetailsAdvice(new TraceIdProvider(
                    new StaticListableBeanFactory().getBeanProvider(io.micrometer.tracing.Tracer.class))))
            .build();

    @Test
    void givenUnexpectedException_whenGet_thenInternalErrorWithoutInternals() throws Exception {
        mockMvc.perform(get("/t/boom"))
                .andExpect(status().isInternalServerError())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.code").value("INTERNAL_ERROR"))
                .andExpect(jsonPath("$.status").value(500))
                .andExpect(jsonPath("$.type").value(ErrorCode.INTERNAL_ERROR.type()))
                .andExpect(jsonPath("$.traceId").isNotEmpty())
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("SELECT"))))
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("IllegalStateException"))))
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("\"at "))));
    }

    @Test
    void givenInvalidBody_whenPost_thenValidationFailedWithFieldErrors() throws Exception {
        mockMvc.perform(post("/t/body").contentType(APPLICATION_JSON).content("{\"name\":\"\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.errors[0].field").value("name"))
                .andExpect(jsonPath("$.errors[0].message").isNotEmpty())
                .andExpect(jsonPath("$.traceId").isNotEmpty());
    }

    @Test
    void givenResponseStatusBadRequest_whenGet_thenValidationFailed() throws Exception {
        mockMvc.perform(get("/t/status"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.detail").value("deviceId: плохой"));
    }

    @Test
    void givenDeviceNotFound_whenGet_thenNotFoundProblem() throws Exception {
        mockMvc.perform(get("/t/missing"))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.code").value("DEVICE_NOT_FOUND"))
                .andExpect(jsonPath("$.type").value(ErrorCode.DEVICE_NOT_FOUND.type()))
                .andExpect(jsonPath("$.traceId").isNotEmpty());
    }

    @Test
    void givenUnknownResource_whenGet_thenNotFoundProblemNotInternalError() throws Exception {
        // Неизвестный путь (например, actuator на основном порту) - 404 Problem Details, а не 500
        mockMvc.perform(get("/t/no-resource"))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.status").value(404))
                .andExpect(jsonPath("$.traceId").isNotEmpty());
    }

    @Test
    void givenReadModelUnavailable_whenGet_thenServiceUnavailableWithoutInternals() throws Exception {
        mockMvc.perform(get("/t/read-model"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.code").value("READ_MODEL_UNAVAILABLE"))
                .andExpect(jsonPath("$.traceId").isNotEmpty())
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("SELECT"))));
    }

    @Test
    void givenMethodNotSupported_whenPost_thenMethodNotAllowedProblemWithAllowHeader() throws Exception {
        // 405 - собственный статус (не 500), Problem Details, заголовок Allow сохранён
        mockMvc.perform(post("/t/get-only"))
                .andExpect(status().isMethodNotAllowed())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.status").value(405))
                .andExpect(jsonPath("$.traceId").isNotEmpty())
                .andExpect(header().string("Allow", org.hamcrest.Matchers.containsString("GET")));
    }

    @Test
    void givenMediaTypeNotSupported_whenPostTextPlain_thenUnsupportedMediaTypeProblem() throws Exception {
        // 415 - собственный статус (не 500), Problem Details
        mockMvc.perform(post("/t/json-only").contentType("text/plain").content("x"))
                .andExpect(status().isUnsupportedMediaType())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.status").value(415))
                .andExpect(jsonPath("$.traceId").isNotEmpty());
    }

    @Test
    void givenMediaTypeNotAcceptable_whenGetXml_thenNotAcceptableProblem() throws Exception {
        // 406 - собственный статус (не 500), Problem Details
        mockMvc.perform(get("/t/json-producer").accept("application/xml"))
                .andExpect(status().isNotAcceptable())
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.status").value(406))
                .andExpect(jsonPath("$.traceId").isNotEmpty());
    }

    @Test
    void givenErrorCode_whenBuilt_thenStatusAndTitleFromEnum() {
        assertThat(ErrorCode.DEVICE_ALREADY_EXISTS.status()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(ErrorCode.UNAUTHORIZED.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(ErrorCode.FORBIDDEN.status()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(ErrorCode.SERVICE_TOKEN_REQUIRED.status()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(ErrorCode.READ_MODEL_UNAVAILABLE.status()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(ErrorCode.INTERNAL_ERROR.title()).isNotBlank();
    }

    @RestController
    static class FailingController {

        @GetMapping("/t/boom")
        String boom() {
            throw new IllegalStateException("SELECT * FROM devices: внутренняя подробность");
        }

        @GetMapping("/t/no-resource")
        String noResource() throws org.springframework.web.servlet.resource.NoResourceFoundException {
            throw new org.springframework.web.servlet.resource.NoResourceFoundException(
                    org.springframework.http.HttpMethod.GET, "t/no-resource", "t/no-resource");
        }

        @PostMapping("/t/body")
        String body(@Valid @RequestBody Body body) {
            return "ok";
        }

        @GetMapping("/t/status")
        String status() {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "deviceId: плохой");
        }

        @GetMapping("/t/missing")
        String missing() {
            throw new DeviceNotFoundException("dev-1");
        }

        @GetMapping("/t/read-model")
        String readModel() {
            throw new ReadModelUnavailableException("SELECT * FROM devices", new IllegalStateException("connect"));
        }

        @GetMapping("/t/get-only")
        String getOnly() {
            return "ok";
        }

        @PostMapping(value = "/t/json-only", consumes = "application/json")
        String jsonOnly(@RequestBody String body) {
            return "ok";
        }

        @GetMapping(value = "/t/json-producer", produces = "application/json")
        String jsonProducer() {
            return "ok";
        }
    }

    record Body(@NotBlank String name) {
    }
}
