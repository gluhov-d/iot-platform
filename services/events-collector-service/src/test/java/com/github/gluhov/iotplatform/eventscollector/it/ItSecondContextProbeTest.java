package com.github.gluhov.iotplatform.eventscollector.it;

import com.github.gluhov.iotplatform.eventscollector.config.KafkaTestContainerConfig;
import com.github.gluhov.iotplatform.failedeventsprocessor.adapter.in.kafka.FailedEventListener;
import org.junit.jupiter.api.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.lifecycle.Startables;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Зонд R23 (T012): процессор failed-events поднимается вторым Spring-контекстом в той же JVM,
 * что и сборщик. Проверяется: контекст стартует, в нём есть FailedEventListener, конфигурация
 * читается из failed-events-processor.yaml (а не из application.yaml сборщика).
 */
class ItSecondContextProbeTest {

    @Test
    void givenProcessorSecondContext_whenStarted_thenListenerPresentAndProcessorYamlUsed() {
        Startables.deepStart(KafkaTestContainerConfig.KAFKA_CONTAINER).join();

        try (ConfigurableApplicationContext ctx = FailedEventsProcessorContext.start(
                KafkaTestContainerConfig.KAFKA_CONTAINER.getBootstrapServers(),
                "http://localhost:8081", "failed-events-probe", "failed-events-probe",
                "http://localhost:9000", "failed-events", "probe", "probe")) {
            assertThat(ctx.getBean(FailedEventListener.class)).isNotNull();
            assertThat(ctx.getEnvironment().getProperty("spring.application.name"))
                    .isEqualTo("failed-events-processor");
            assertThat(ctx.getEnvironment().getProperty("failed-events.bucket")).isEqualTo("failed-events");
        }
    }
}
