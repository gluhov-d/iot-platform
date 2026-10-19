package com.github.gluhov.iotplatform.eventscollector.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

// В интеграционных тестах выключается (app.outbox.scheduling-enabled=false), итерации запускает тест
@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnBooleanProperty(name = "app.outbox.scheduling-enabled", matchIfMissing = true)
public class SchedulingConfig {
}
