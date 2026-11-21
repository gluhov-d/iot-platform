package com.github.gluhov.iotplatform.deviceservice.adapter.in.web;

import com.github.gluhov.iotplatform.deviceservice.api.DevicesApi;
import com.github.gluhov.iotplatform.deviceservice.api.model.DeviceCommandAccepted;
import com.github.gluhov.iotplatform.deviceservice.api.model.DeviceCreateRequest;
import com.github.gluhov.iotplatform.deviceservice.api.model.DeviceResponse;
import com.github.gluhov.iotplatform.deviceservice.api.model.DeviceUpdateRequest;
import com.github.gluhov.iotplatform.deviceservice.domain.model.DeviceCommand;
import com.github.gluhov.iotplatform.deviceservice.domain.port.in.GetDeviceUseCase;
import com.github.gluhov.iotplatform.deviceservice.domain.port.in.ManageDeviceUseCase;
import jakarta.validation.Valid;
import jakarta.validation.Validator;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;

/**
 * Реализация DevicesApi. Маршруты берутся из интерфейса (аннотации @HttpExchange), собственного @RequestMapping нет.
 * Тело проверяется @Valid на параметре реализации; deviceId из пути проверяется явно: ограничения на @PathVariable
 * в реализации дают ошибку объявления (research.md R4).
 * Все четыре метода контракта реализованы: чтение (US1), изменение (US2), создание (US3), удаление (US4).
 */
@RestController
public class DeviceController implements DevicesApi {

    private static final ContractMapValuesValidator MAP_VALUES = new ContractMapValuesValidator();

    private final Validator validator;
    private final GetDeviceUseCase getDevice;
    private final ManageDeviceUseCase manageDevice;
    private final DeviceMapper deviceMapper;

    public DeviceController(Validator validator, GetDeviceUseCase getDevice, ManageDeviceUseCase manageDevice,
                            DeviceMapper deviceMapper) {
        this.validator = validator;
        this.getDevice = getDevice;
        this.manageDevice = manageDevice;
        this.deviceMapper = deviceMapper;
    }

    /** Длина значений settings/attributes (контракт): проверка тела вместе с @Valid, ошибка - в errors[]. */
    @InitBinder
    void contractMapValues(WebDataBinder binder) {
        Object target = binder.getTarget();
        if (target != null && MAP_VALUES.supports(target.getClass())) {
            binder.addValidators(MAP_VALUES);
        }
    }

    @Override
    public ResponseEntity<DeviceCommandAccepted> createDevice(@Valid DeviceCreateRequest deviceCreateRequest) {
        checkDeviceId(deviceCreateRequest.getDeviceId());
        DeviceCommand command = manageDevice.create(deviceCreateRequest.getDeviceId(),
                deviceMapper.toFields(deviceCreateRequest));
        return accepted(command);
    }

    @Override
    public ResponseEntity<DeviceResponse> getDeviceById(String deviceId) {
        checkDeviceId(deviceId);
        return ResponseEntity.ok(deviceMapper.toResponse(getDevice.getById(deviceId)));
    }

    @Override
    public ResponseEntity<DeviceCommandAccepted> updateDevice(String deviceId, @Valid DeviceUpdateRequest deviceUpdateRequest) {
        checkDeviceId(deviceId);
        DeviceCommand command = manageDevice.update(deviceId, deviceMapper.toFields(deviceUpdateRequest));
        return accepted(command);
    }

    @Override
    public ResponseEntity<DeviceCommandAccepted> deleteDevice(String deviceId) {
        checkDeviceId(deviceId);
        return accepted(manageDevice.delete(deviceId));
    }

    /** 202: команда подтверждена Kafka; Location - адрес устройства для последующего чтения. */
    private ResponseEntity<DeviceCommandAccepted> accepted(DeviceCommand command) {
        URI location = UriComponentsBuilder.fromPath("/api/v1/devices/{deviceId}")
                .buildAndExpand(command.deviceId())
                .encode()
                .toUri();
        return ResponseEntity.accepted().location(location).body(deviceMapper.toAccepted(command));
    }

    // Правила deviceId - как в контракте: 1–128 символов, без «/» и пробельных символов
    private void checkDeviceId(String deviceId) {
        var violations = validator.validate(new DeviceIdPath(deviceId));
        if (!violations.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, violations.iterator().next().getMessage());
        }
    }

    // Носитель проверок для одного параметра пути (сами ограничения - только здесь, не на @PathVariable)
    private record DeviceIdPath(
            @NotBlank
            @Size(max = 128)
            @Pattern(regexp = "^[^/\\s]+$")
            String deviceId
    ) {
    }
}
