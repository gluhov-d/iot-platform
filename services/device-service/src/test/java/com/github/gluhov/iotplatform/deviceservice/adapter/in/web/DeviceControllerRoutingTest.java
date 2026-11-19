package com.github.gluhov.iotplatform.deviceservice.adapter.in.web;

import com.github.gluhov.iotplatform.deviceservice.domain.exception.DeviceNotFoundException;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceCommand;
import com.github.gluhov.iotplatform.deviceservice.domain.port.in.GetDeviceUseCase;
import com.github.gluhov.iotplatform.deviceservice.domain.port.in.ManageDeviceUseCase;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.net.URI;
import java.time.Instant;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Маршруты контракта: четыре метода находят обработчик (ответ 202 или 200 с 404 для GET, не 404 и не 405 для самого маршрута); невалидное тело и
 * deviceId длиннее 128 символов - 400. Фильтры безопасности отключены: проверка токенов - в ItSecurityTest.
 */
@WebMvcTest(DeviceController.class)
@AutoConfigureMockMvc(addFilters = false)
// Обработчик ошибок требует идентификатор трассировки (TraceIdProvider - компонент, в срез не попадает);
// маппер - реализация MapStruct (компонент сборки, в срез не попадает)
@Import({TraceIdProvider.class, DeviceMapperImpl.class})
class DeviceControllerRoutingTest {

    private static final String VALID_CREATE =
            "{\"deviceId\":\"dev-1\",\"tenantId\":\"t1\",\"deviceType\":\"sensor\",\"status\":\"NEW\"}";
    private static final String VALID_UPDATE =
            "{\"tenantId\":\"t1\",\"deviceType\":\"sensor\",\"status\":\"ACTIVE\"}";
    private static final String TOO_LONG_DEVICE_ID = "d".repeat(129);

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private GetDeviceUseCase getDeviceUseCase;

    @MockitoBean
    private ManageDeviceUseCase manageDeviceUseCase;

    @Test
    void givenUnknownDevice_whenGet_thenNotFoundProblem() throws Exception {
        when(getDeviceUseCase.getById("dev-1")).thenThrow(new DeviceNotFoundException("dev-1"));

        mockMvc.perform(get("/api/v1/devices/dev-1"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value("DEVICE_NOT_FOUND"));
    }

    @Test
    void givenContractMethods_whenCalled_thenAcceptedWithOperation() throws Exception {
        when(manageDeviceUseCase.create(anyString(), any())).thenReturn(accepted(DeviceCommand.Operation.CREATE));
        mockMvc.perform(post("/api/v1/devices").contentType(APPLICATION_JSON).content(VALID_CREATE))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.operation").value("CREATE"));
        when(manageDeviceUseCase.update(anyString(), any())).thenReturn(accepted(DeviceCommand.Operation.UPDATE));
        mockMvc.perform(put("/api/v1/devices/dev-1").contentType(APPLICATION_JSON).content(VALID_UPDATE))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.operation").value("UPDATE"));
        when(manageDeviceUseCase.delete(anyString())).thenReturn(accepted(DeviceCommand.Operation.DELETE));
        mockMvc.perform(delete("/api/v1/devices/dev-1"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.operation").value("DELETE"));
    }

    @Test
    void givenInvalidBody_whenPost_thenBadRequest() throws Exception {
        mockMvc.perform(post("/api/v1/devices").contentType(APPLICATION_JSON).content("{}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void givenInvalidBody_whenPut_thenBadRequest() throws Exception {
        mockMvc.perform(put("/api/v1/devices/dev-1").contentType(APPLICATION_JSON).content("{}"))
                .andExpect(status().isBadRequest());
    }

    private static DeviceCommand accepted(DeviceCommand.Operation operation) {
        return new DeviceCommand(UUID.randomUUID(), operation, "dev-1", null, Instant.now());
    }

    @Test
    void givenDeviceIdLongerThan128_whenGet_thenBadRequest() throws Exception {
        mockMvc.perform(get("/api/v1/devices/" + TOO_LONG_DEVICE_ID))
                .andExpect(status().isBadRequest());
    }

    @Test
    void givenDeviceIdLongerThan128_whenPut_thenBadRequest() throws Exception {
        mockMvc.perform(put("/api/v1/devices/" + TOO_LONG_DEVICE_ID).contentType(APPLICATION_JSON).content(VALID_UPDATE))
                .andExpect(status().isBadRequest());
    }

    @Test
    void givenDeviceIdLongerThan128_whenDelete_thenBadRequest() throws Exception {
        mockMvc.perform(delete("/api/v1/devices/" + TOO_LONG_DEVICE_ID))
                .andExpect(status().isBadRequest());
    }

    @Test
    void givenDeviceIdWithSpaceInPath_whenGet_thenValidationFailed() throws Exception {
        mockMvc.perform(get(URI.create("/api/v1/devices/dev%201")))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));
    }

    @Test
    void givenDeviceIdWithSlashInPath_whenPut_thenValidationFailed() throws Exception {
        mockMvc.perform(put(URI.create("/api/v1/devices/dev%2F1")).contentType(APPLICATION_JSON).content(VALID_UPDATE))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));
    }

    @Test
    void givenDeviceIdWithSpaceInBody_whenPost_thenValidationFailed() throws Exception {
        String body = "{\"deviceId\":\"dev 1\",\"tenantId\":\"t1\",\"deviceType\":\"sensor\",\"status\":\"NEW\"}";

        mockMvc.perform(post("/api/v1/devices").contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));
    }

    @Test
    void givenDeviceIdWithSlashInBody_whenPost_thenValidationFailed() throws Exception {
        String body = "{\"deviceId\":\"dev/1\",\"tenantId\":\"t1\",\"deviceType\":\"sensor\",\"status\":\"NEW\"}";

        mockMvc.perform(post("/api/v1/devices").contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"));
    }

    // Контракт: settings и attributes - не больше 100 ключей, значение не длиннее 1024 символов (maxProperties, maxLength)
    @Test
    void givenTooManySettings_whenPut_thenValidationFailed() throws Exception {
        String body = "{\"tenantId\":\"t1\",\"deviceType\":\"sensor\",\"status\":\"ACTIVE\",\"settings\":" + map(101, 1) + "}";

        mockMvc.perform(put("/api/v1/devices/dev-1").contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.errors[0].field").value("settings"));
        verifyNoInteractions(manageDeviceUseCase);
    }

    @Test
    void givenTooLongAttributeValue_whenPost_thenValidationFailed() throws Exception {
        String body = "{\"deviceId\":\"dev-1\",\"tenantId\":\"t1\",\"deviceType\":\"sensor\",\"status\":\"NEW\",\"attributes\":"
                + map(1, 1025) + "}";

        mockMvc.perform(post("/api/v1/devices").contentType(APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"))
                .andExpect(jsonPath("$.errors[0].field").value("attributes"));
        verifyNoInteractions(manageDeviceUseCase);
    }

    @Test
    void givenMapsAtContractLimits_whenPostAndPut_thenAccepted() throws Exception {
        String maps = ",\"settings\":" + map(100, 1024) + ",\"attributes\":" + map(100, 1024) + "}";
        when(manageDeviceUseCase.create(anyString(), any())).thenReturn(accepted(DeviceCommand.Operation.CREATE));
        when(manageDeviceUseCase.update(anyString(), any())).thenReturn(accepted(DeviceCommand.Operation.UPDATE));

        mockMvc.perform(post("/api/v1/devices").contentType(APPLICATION_JSON)
                        .content(VALID_CREATE.substring(0, VALID_CREATE.length() - 1) + maps))
                .andExpect(status().isAccepted());
        mockMvc.perform(put("/api/v1/devices/dev-1").contentType(APPLICATION_JSON)
                        .content(VALID_UPDATE.substring(0, VALID_UPDATE.length() - 1) + maps))
                .andExpect(status().isAccepted());
    }

    // FR-011: null не определён в контракте - ни у самой карты, ни у её значений; ошибка поля, публикации нет
    @Test
    void givenNullMapOrNullValue_whenPostAndPut_thenValidationFailedWithField() throws Exception {
        for (String field : new String[]{"settings", "attributes"}) {
            for (String map : new String[]{"null", "{\"k\":null}", "{\"k\":\"v\",\"n\":null}"}) {
                String tail = ",\"" + field + "\":" + map + "}";
                mockMvc.perform(post("/api/v1/devices").contentType(APPLICATION_JSON)
                                .content(VALID_CREATE.substring(0, VALID_CREATE.length() - 1) + tail))
                        .andExpect(status().isBadRequest())
                        .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"))
                        .andExpect(jsonPath("$.errors[0].field").value(field));
                mockMvc.perform(put("/api/v1/devices/dev-1").contentType(APPLICATION_JSON)
                                .content(VALID_UPDATE.substring(0, VALID_UPDATE.length() - 1) + tail))
                        .andExpect(status().isBadRequest())
                        .andExpect(jsonPath("$.code").value("VALIDATION_FAILED"))
                        .andExpect(jsonPath("$.errors[0].field").value(field));
            }
        }
        verifyNoInteractions(manageDeviceUseCase);
    }

    @Test
    void givenNoSettingsAndAttributes_whenPostAndPut_thenAcceptedWithEmptyMaps() throws Exception {
        when(manageDeviceUseCase.create(anyString(), any())).thenReturn(accepted(DeviceCommand.Operation.CREATE));
        when(manageDeviceUseCase.update(anyString(), any())).thenReturn(accepted(DeviceCommand.Operation.UPDATE));

        mockMvc.perform(post("/api/v1/devices").contentType(APPLICATION_JSON).content(VALID_CREATE))
                .andExpect(status().isAccepted());
        mockMvc.perform(put("/api/v1/devices/dev-1").contentType(APPLICATION_JSON).content(VALID_UPDATE))
                .andExpect(status().isAccepted());

        ArgumentCaptor<DeviceCommand.Fields> fields = ArgumentCaptor.forClass(DeviceCommand.Fields.class);
        verify(manageDeviceUseCase).create(anyString(), fields.capture());
        verify(manageDeviceUseCase).update(anyString(), fields.capture());
        assertThat(fields.getAllValues()).allSatisfy(f -> {
            assertThat(f.settings()).isEmpty();
            assertThat(f.attributes()).isEmpty();
        });
    }

    // JSON-объект из keys ключей k0..kN, значение каждого - valueLength символов
    private static String map(int keys, int valueLength) {
        String value = "v".repeat(valueLength);
        return IntStream.range(0, keys)
                .mapToObj(i -> "\"k" + i + "\":\"" + value + "\"")
                .collect(Collectors.joining(",", "{", "}"));
    }
}
