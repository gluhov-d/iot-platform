package com.github.gluhov.iotplatform.failedeventsprocessor.it;

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * Спаны в памяти для проверки трассы записи (T039): Boot подключает бин SpanExporter к поставщику трасс.
 * Подключается через @Import на AbstractItTest: вложенная конфигурация суперкласса Spring не находит.
 */
@TestConfiguration(proxyBeanMethods = false)
public class SpanCaptureConfig {

    @Bean
    InMemorySpanExporter inMemorySpanExporter() {
        return InMemorySpanExporter.create();
    }
}
