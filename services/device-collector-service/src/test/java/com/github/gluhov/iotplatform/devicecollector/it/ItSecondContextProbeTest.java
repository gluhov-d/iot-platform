package com.github.gluhov.iotplatform.devicecollector.it;

import com.github.gluhov.iotplatform.failedeventsprocessor.FailedEventsProcessorApplication;
import com.github.gluhov.iotplatform.failedeventsprocessor.adapter.in.kafka.FailedEventListener;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Зонд R23 (T012, T023): процессор поднимается вторым Spring-контекстом в той же JVM, что и контекст сборщика.
 * Проверяется: контекст поднимается, в нём есть FailedEventListener, конфигурация читается из
 * failed-events-processor.yaml (а не из application.yaml сборщика), глобальное свойство spring.config.name не
 * выставляется (оно повлияло бы на контексты, стартующие позже).
 */
class ItSecondContextProbeTest extends AbstractItTest {

    @Test
    void givenProcessorAsSecondContext_whenStarted_thenListenerAndOwnConfigAndNoGlobalConfigName() {
        assertThat(System.getProperty("spring.config.name")).as("глобальное свойство JVM не выставляется").isNull();

        try (ConfigurableApplicationContext processor = new SpringApplicationBuilder(FailedEventsProcessorApplication.class)
                .web(WebApplicationType.NONE)
                .properties(failedEventsProcessorProperties())
                .run()) {
            assertThat(processor.getBean(FailedEventListener.class)).isNotNull();
            assertThat(processor.getEnvironment().getProperty("spring.application.name"))
                    .as("имя из failed-events-processor.yaml, а не из application.yaml сборщика")
                    .isEqualTo("failed-events-processor");
            assertThat(processor.getEnvironment().getProperty("failed-events.bucket")).isEqualTo(BUCKET);
        }

        assertThat(System.getProperty("spring.config.name")).isNull();
    }
}
