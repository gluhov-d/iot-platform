package com.github.gluhov.iotplatform.deviceservice.adapter.in.web;

import com.github.gluhov.iotplatform.deviceservice.api.model.DeviceCreateRequest;
import com.github.gluhov.iotplatform.deviceservice.api.model.DeviceUpdateRequest;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

import java.util.Map;

/**
 * Ограничение контракта, которое генератор не переносит в модели: длина значения в settings и attributes не больше
 * 1024 символов (additionalProperties.maxLength). Число ключей (maxProperties) генератор проверяет сам - @Size на геттере.
 * Подключается к проверке тела через @InitBinder, поэтому ошибка попадает в errors[] ответа VALIDATION_FAILED.
 */
public class ContractMapValuesValidator implements Validator {

    static final int MAX_VALUE_LENGTH = 1024;

    @Override
    public boolean supports(Class<?> clazz) {
        return DeviceCreateRequest.class.isAssignableFrom(clazz) || DeviceUpdateRequest.class.isAssignableFrom(clazz);
    }

    @Override
    public void validate(Object target, Errors errors) {
        if (target instanceof DeviceCreateRequest create) {
            check("settings", create.getSettings(), errors);
            check("attributes", create.getAttributes(), errors);
        } else if (target instanceof DeviceUpdateRequest update) {
            check("settings", update.getSettings(), errors);
            check("attributes", update.getAttributes(), errors);
        }
    }

    private static void check(String field, Map<String, String> values, Errors errors) {
        if (values == null) {
            return;
        }
        boolean tooLong = values.values().stream().anyMatch(v -> v != null && v.length() > MAX_VALUE_LENGTH);
        if (tooLong) {
            errors.rejectValue(field, "Size", "values must be at most " + MAX_VALUE_LENGTH + " characters");
        }
    }
}
