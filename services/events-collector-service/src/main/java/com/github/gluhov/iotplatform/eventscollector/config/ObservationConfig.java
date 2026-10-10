package com.github.gluhov.iotplatform.eventscollector.config;

import io.micrometer.observation.ObservationPredicate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.observation.ServerRequestObservationContext;

@Configuration(proxyBeanMethods = false)
public class ObservationConfig {

    // Убирает шум из трассировок: запросы /actuator/* (Prometheus, healthcheck) и спаны команд
    // Redis (Lettuce) - их корневые спаны от healthcheck засоряли бы Tempo. Шаги с Redis видны
    // через собственные спаны сервиса: "redis devices:published check" и "...add".
    @Bean
    ObservationPredicate skipTechnicalObservations() {
        return (name, context) -> {
            if (context instanceof ServerRequestObservationContext http) {
                return !http.getCarrier().getRequestURI().startsWith("/actuator");
            }
            return !name.startsWith("lettuce");
        };
    }
}
