package com.github.gluhov.iotplatform.devicecollector.config;

import io.micrometer.observation.ObservationPredicate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.observation.ServerRequestObservationContext;

@Configuration(proxyBeanMethods = false)
public class ObservationConfig {

    // Запросы /actuator/* (Prometheus, healthcheck) не трассируются: их корневые спаны засоряли бы Tempo
    @Bean
    ObservationPredicate skipActuatorObservations() {
        return (name, context) -> !(context instanceof ServerRequestObservationContext http)
                || !http.getCarrier().getRequestURI().startsWith("/actuator");
    }
}