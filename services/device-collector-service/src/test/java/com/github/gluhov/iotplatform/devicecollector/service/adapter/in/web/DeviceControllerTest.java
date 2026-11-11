package com.github.gluhov.iotplatform.devicecollector.service.adapter.in.web;

import com.github.gluhov.iotplatform.devicecollector.adapter.in.web.DeviceController;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.StorageAccessException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceState;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceView;
import com.github.gluhov.iotplatform.devicecollector.domain.model.SensorState;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StorageFailureReason;
import com.github.gluhov.iotplatform.devicecollector.domain.port.in.FindDeviceUseCase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.github.gluhov.iotplatform.devicecollector.data.TestData.T0;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class DeviceControllerTest {
    private final FindDeviceUseCase findDevice = mock(FindDeviceUseCase.class);
    private final MockMvc mvc = MockMvcBuilders.standaloneSetup(new DeviceController(findDevice)).build();

    @Test
    @DisplayName("Known device: 200 with the model, counters and the shard that returned the row")
    void givenKnownDevice_whenGet_thenDevice() throws Exception {
        DeviceState state = new DeviceState("dev-1", "tenant-1", null, "thermometer", null, null, null, "1.0.0",
                "Boiler room sensor", "Linux", "6.1",
                "ACTIVE", null, T0, Map.of("mode", "eco"), Map.of(),
                List.of(new SensorState("t1", "TEMPERATURE", "C", "21.5", T0, "OK")));
        when(findDevice.find("dev-1")).thenReturn(Optional.of(
                new DeviceView(state, 3, T0, T0, "evt-3", "UPDATED", null, "shard_1")));

        mvc.perform(get("/api/v1/devices/dev-1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.deviceId").value("dev-1"))
                .andExpect(jsonPath("$.status").value("ACTIVE"))
                .andExpect(jsonPath("$.name").value("Boiler room sensor"))
                .andExpect(jsonPath("$.osName").value("Linux"))
                .andExpect(jsonPath("$.osVersion").value("6.1"))
                .andExpect(jsonPath("$.settings.mode").value("eco"))
                .andExpect(jsonPath("$.sensors[0].sensorId").value("t1"))
                .andExpect(jsonPath("$.seenCount").value(3))
                .andExpect(jsonPath("$.lastEventId").value("evt-3"))
                .andExpect(jsonPath("$.shard").value("shard_1"));
    }

    @Test
    @DisplayName("Unknown device: 404 problem")
    void givenUnknownDevice_whenGet_thenNotFound() throws Exception {
        when(findDevice.find("nope")).thenReturn(Optional.empty());

        mvc.perform(get("/api/v1/devices/nope"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.detail").value(containsString("nope")));
    }

    @Test
    @DisplayName("deviceId longer than 128 characters: 400 without a database query")
    void givenTooLongId_whenGet_thenBadRequest() throws Exception {
        mvc.perform(get("/api/v1/devices/" + "d".repeat(129)))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(findDevice);
    }

    @Test
    @DisplayName("Shard or Proxy unavailable: 503 without internal details")
    void givenStorageUnavailable_whenGet_thenServiceUnavailable() throws Exception {
        when(findDevice.find("dev-1")).thenThrow(new StorageAccessException(StorageFailureReason.CONNECTION,
                "Select device failed: HikariPool-2 - Connection is not available", null));

        mvc.perform(get("/api/v1/devices/dev-1"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.detail").value("Device registry is unavailable"));
    }
}