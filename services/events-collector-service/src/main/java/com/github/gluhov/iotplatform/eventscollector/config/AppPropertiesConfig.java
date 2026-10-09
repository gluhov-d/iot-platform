package com.github.gluhov.iotplatform.eventscollector.config;

import com.github.gluhov.iotplatform.eventscollector.domain.model.OutboxPolicy;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.time.Duration;

@Configuration
@EnableConfigurationProperties({KafkaProperties.class, OutboxProperties.class})
public class AppPropertiesConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    OutboxPolicy outboxPolicy(OutboxProperties outboxProperties) {
        return new OutboxPolicy(outboxProperties.batchSize(), outboxProperties.maxAttempts(),
                Duration.ofMillis(outboxProperties.leaseTtlMs()));
    }
}
