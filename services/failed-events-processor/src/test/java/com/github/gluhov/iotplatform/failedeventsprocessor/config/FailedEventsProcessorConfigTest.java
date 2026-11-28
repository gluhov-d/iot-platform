package com.github.gluhov.iotplatform.failedeventsprocessor.config;

import com.github.gluhov.iotplatform.failedeventsprocessor.FailedEventsProcessorApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Конфигурация только из переменных окружения (FR-022, R23): файл failed-events-processor.yaml, без application.yaml,
 * значений по умолчанию в плейсхолдерах.
 */
class FailedEventsProcessorConfigTest {

    private static final Pattern PLACEHOLDER_WITH_DEFAULT = Pattern.compile("\\$\\{[^}]*:[^}]*}");

    @Test
    void givenProcessorConfigFile_whenRead_thenItExistsAndNameIsProcessor() {
        Properties yaml = load("failed-events-processor.yaml");

        assertThat(yaml.getProperty("spring.application.name")).isEqualTo("failed-events-processor");
    }

    @Test
    void givenProcessorConfig_whenRead_thenAllValuesAreEnvironmentPlaceholdersWithoutDefaults() {
        Properties yaml = load("failed-events-processor.yaml");

        assertThat(yaml.stringPropertyNames())
                .filteredOn(name -> yaml.getProperty(name).contains("${"))
                .allSatisfy(name -> assertThat(yaml.getProperty(name))
                        .as("значение %s - переменная без значения по умолчанию", name)
                        .matches("\\$\\{[A-Z0-9_]+}"));
        assertThat(yaml.stringPropertyNames())
                .filteredOn(name -> PLACEHOLDER_WITH_DEFAULT.matcher(yaml.getProperty(name)).find())
                .isEmpty();
    }

    @Test
    void givenProcessorConfig_whenRead_thenKafkaConsumerSettingsAsRequired() {
        Properties yaml = load("failed-events-processor.yaml");

        assertThat(yaml.getProperty("spring.kafka.consumer.enable-auto-commit")).isEqualTo("false");
        assertThat(yaml.getProperty("spring.kafka.consumer.max-poll-records")).isEqualTo("1");
        assertThat(yaml.getProperty("spring.kafka.consumer.auto-offset-reset")).isEqualTo("earliest");
        assertThat(yaml.getProperty("spring.kafka.listener.ack-mode")).isEqualTo("manual_immediate");
        assertThat(yaml.getProperty("spring.kafka.consumer.properties.max.poll.interval.ms"))
                .isEqualTo("${FAILED_EVENTS_MAX_POLL_INTERVAL_MS}");
        assertThat(yaml.getProperty("spring.kafka.consumer.key-deserializer"))
                .isEqualTo("org.springframework.kafka.support.serializer.ErrorHandlingDeserializer");
        assertThat(yaml.getProperty("spring.kafka.listener.observation-enabled")).isEqualTo("true");
    }

    @Test
    void givenProcessorConfig_whenRead_thenSchemaRegistryClientTimeoutsAndRetriesFromEnvironment() {
        Properties yaml = load("failed-events-processor.yaml");

        assertThat(yaml.getProperty("spring.kafka.properties.schema.registry.http.connect.timeout.ms"))
                .isEqualTo("${FAILED_EVENTS_SCHEMA_REGISTRY_CONNECT_TIMEOUT_MS}");
        assertThat(yaml.getProperty("spring.kafka.properties.schema.registry.http.read.timeout.ms"))
                .isEqualTo("${FAILED_EVENTS_SCHEMA_REGISTRY_READ_TIMEOUT_MS}");
        assertThat(yaml.getProperty("spring.kafka.properties.schema.registry.max.retries"))
                .isEqualTo("${FAILED_EVENTS_SCHEMA_REGISTRY_MAX_RETRIES}");
    }

    /**
     * Бюджет одного цикла опроса (plan.md, «Ограничения»; R25) для значений .env.example меньше max.poll.interval.ms:
     * (повторы + 1) × (соединение + чтение) + тайм-аут S3 + максимальная пауза восстановителя.
     */
    @Test
    void givenEnvExampleValues_whenPollCycleBudgetComputed_thenBelowMaxPollInterval() throws Exception {
        Map<String, String> env = readEnvExample();
        long connectTimeoutMs = millis(env, "FAILED_EVENTS_SCHEMA_REGISTRY_CONNECT_TIMEOUT_MS");
        long readTimeoutMs = millis(env, "FAILED_EVENTS_SCHEMA_REGISTRY_READ_TIMEOUT_MS");
        long maxRetries = millis(env, "FAILED_EVENTS_SCHEMA_REGISTRY_MAX_RETRIES");
        long s3ApiCallTimeoutMs = millis(env, "FAILED_EVENTS_S3_API_CALL_TIMEOUT");
        long retryMaxIntervalMs = millis(env, "FAILED_EVENTS_RETRY_MAX_INTERVAL");
        long maxPollIntervalMs = millis(env, "FAILED_EVENTS_MAX_POLL_INTERVAL_MS");

        long budgetMs = (maxRetries + 1) * (connectTimeoutMs + readTimeoutMs) + s3ApiCallTimeoutMs + retryMaxIntervalMs;

        assertThat(s3ApiCallTimeoutMs).isLessThan(maxPollIntervalMs);
        assertThat(budgetMs).as("бюджет цикла опроса").isLessThan(maxPollIntervalMs);
    }

    /** Значения переменных из .env.example (сам .env не читается). Тайм-ауты - в миллисекундах или секундах (8s), повторы - числом. */
    private static Map<String, String> readEnvExample() throws Exception {
        Map<String, String> values = new HashMap<>();
        for (String line : Files.readAllLines(Path.of("../../.env.example"), StandardCharsets.UTF_8)) {
            String trimmed = line.trim();
            int eq = trimmed.indexOf('=');
            if (!trimmed.startsWith("#") && eq > 0) {
                values.put(trimmed.substring(0, eq), trimmed.substring(eq + 1));
            }
        }
        return values;
    }

    private static long millis(Map<String, String> env, String name) {
        String value = env.get(name);
        assertThat(value).as("переменная %s задана в .env.example", name).isNotNull();
        if (value.endsWith("ms")) {
            return Long.parseLong(value.substring(0, value.length() - 2));
        }
        if (value.endsWith("s")) {
            return Long.parseLong(value.substring(0, value.length() - 1)) * 1_000;
        }
        return Long.parseLong(value);
    }

    @Test
    void givenNoApplicationYaml_whenStarted_thenProcessorUsesItsOwnConfigName() {
        assertThat(new ClassPathResource("application.yaml").exists())
                .as("application.yaml не создаётся: своё имя файла конфигурации (R23)").isFalse();

        FailedEventsProcessorApplication.configureConfigName();

        assertThat(System.getProperty("spring.config.name")).isEqualTo("failed-events-processor");
    }

    private static Properties load(String name) {
        YamlPropertiesFactoryBean factory = new YamlPropertiesFactoryBean();
        factory.setResources(new ClassPathResource(name));
        Properties properties = factory.getObject();
        assertThat(properties).as("файл %s читается", name).isNotNull();
        return properties;
    }
}
