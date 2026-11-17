package com.github.gluhov.iotplatform.deviceservice.adapter.in.web;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Коды ошибок сервиса - ровно значения {@code Problem.code} из контракта OpenAPI (FR-006).
 * Файл контракта читается из модуля device-service-client; рабочий каталог тестов - каталог модуля сервиса.
 */
class ErrorCodeTest {

    private static final Path CONTRACT = Path.of("../device-service-client/src/main/openapi/device-service.yaml");

    @Test
    void givenContractProblemCodes_whenCompared_thenSameSet() throws Exception {
        String contract = Files.readString(CONTRACT);
        // Перечисление в схеме Problem: строка вида "code: ... enum: [A, B, ...]" (список может переноситься на следующую строку)
        int codeIndex = contract.indexOf("        code:\n          type: string\n          enum: [");
        assertThat(codeIndex).as("enum кодов в схеме Problem").isNotNegative();
        int start = contract.indexOf('[', codeIndex);
        int end = contract.indexOf(']', start);
        Set<String> fromContract = Arrays.stream(contract.substring(start + 1, end).split(","))
                .map(s -> s.replaceAll("\\s", ""))
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());

        Set<String> fromCode = Arrays.stream(ErrorCode.values())
                .map(Enum::name)
                .collect(Collectors.toSet());

        assertThat(fromCode).isEqualTo(fromContract);
    }

    @Test
    void givenEachCode_whenAskedForType_thenUriUsesKebabCase() {
        assertThat(ErrorCode.DEVICE_NOT_FOUND.type()).endsWith("/problems/device-not-found");
        assertThat(ErrorCode.SERVICE_TOKEN_REQUIRED.type()).endsWith("/problems/service-token-required");
    }
}
