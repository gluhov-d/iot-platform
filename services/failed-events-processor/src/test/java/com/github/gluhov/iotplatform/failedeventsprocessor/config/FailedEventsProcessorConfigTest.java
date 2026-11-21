package com.github.gluhov.iotplatform.failedeventsprocessor.config;

import com.github.gluhov.iotplatform.failedeventsprocessor.FailedEventsProcessorApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

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
