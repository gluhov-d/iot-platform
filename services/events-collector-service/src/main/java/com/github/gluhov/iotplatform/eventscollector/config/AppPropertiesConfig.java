package com.github.gluhov.iotplatform.eventscollector.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({KafkaProperties.class, OutboxProperties.class})
public class AppPropertiesConfig {
}
