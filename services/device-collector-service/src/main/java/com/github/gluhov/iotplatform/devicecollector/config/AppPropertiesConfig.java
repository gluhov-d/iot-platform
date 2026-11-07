package com.github.gluhov.iotplatform.devicecollector.config;

import com.github.gluhov.iotplatform.devicecollector.domain.model.ValidationPolicy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties({KafkaProperties.class, ProcessingProperties.class})
public class AppPropertiesConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    ValidationPolicy validationPolicy(ProcessingProperties processing) {
        return new ValidationPolicy(processing.maxFutureSkew());
    }
}