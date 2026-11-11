package com.github.gluhov.iotplatform.devicecollector.adapter.in.web;

import com.github.gluhov.iotplatform.devicecollector.domain.exception.StorageAccessException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.DeviceChange;
import com.github.gluhov.iotplatform.devicecollector.domain.port.in.FindDeviceUseCase;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Только чтение по ключу шардирования - запрос уходит на один шард. Создания устройств и списков нет
 * (запрещено заданием): реестр наполняется только из Kafka.
 */
@RestController
@RequestMapping("/api/v1/devices")
@RequiredArgsConstructor
@Slf4j
public class DeviceController {
    private final FindDeviceUseCase findDevice;

    @GetMapping("/{deviceId}")
    public ResponseEntity<?> get(@PathVariable String deviceId) {
        if (deviceId.isBlank() || deviceId.length() > DeviceChange.MAX_DEVICE_ID_LENGTH) {
            return ResponseEntity.of(ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                    "deviceId must be 1.." + DeviceChange.MAX_DEVICE_ID_LENGTH + " characters")).build();
        }
        return findDevice.find(deviceId)
                .<ResponseEntity<?>>map(view -> ResponseEntity.ok(DeviceResponse.from(view)))
                .orElseGet(() -> ResponseEntity.of(ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND,
                        "Device " + deviceId + " not found")).build());
    }

    // Шард или Proxy недоступны: 503, подробности - в журнале сервиса, не в ответе
    @ExceptionHandler(StorageAccessException.class)
    ResponseEntity<ProblemDetail> storageUnavailable(StorageAccessException e) {
        log.warn("Device registry unavailable ({}): {}", e.reason().label(), e.getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, "Device registry is unavailable"));
    }
}