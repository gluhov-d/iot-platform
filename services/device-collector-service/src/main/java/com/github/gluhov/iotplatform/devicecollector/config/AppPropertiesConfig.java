package com.github.gluhov.iotplatform.devicecollector.config;

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
}