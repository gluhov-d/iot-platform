package com.github.gluhov.iotplatform.eventscollector.adapter.out.kafka;

import com.github.gluhov.iotplatform.eventscollector.config.KafkaProperties;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.CollectorMetrics;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Tracer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FailedEventRecovererConfigTest {

    @Test
    @DisplayName("givenFailedEventsProducer_whenCreated_thenObservationEnabledSoTraceparentIsInjected")
    void givenFailedEventsProducer_whenCreated_thenObservationEnabledSoTraceparentIsInjected() {
        @SuppressWarnings("unchecked")
        ProducerFactory<String, Object> base = mock(ProducerFactory.class);
        @SuppressWarnings("unchecked")
        ProducerFactory<String, Object> copy = mock(ProducerFactory.class);
        when(base.copyWithConfigurationOverride(anyMap())).thenReturn(copy);
        ObservationRegistry registry = ObservationRegistry.create();

        FailedEventRecoverer recoverer = new FailedEventRecovererConfig().failedEventRecoverer(
                base, mock(Tracer.class), registry, mock(CollectorMetrics.class),
                new KafkaProperties("events", "devices", "failed-events", Duration.ofSeconds(20)),
                "events-collector-service", Duration.ofSeconds(5), 1000L, 1000, 2000);

        Object template = ReflectionTestUtils.getField(recoverer, "template");
        assertThat(template).isInstanceOf(KafkaTemplate.class);
        assertThat(ReflectionTestUtils.getField(template, "observationEnabled")).isEqualTo(true);
        assertThat(ReflectionTestUtils.getField(template, "observationRegistry")).isSameAs(registry);
    }
}
