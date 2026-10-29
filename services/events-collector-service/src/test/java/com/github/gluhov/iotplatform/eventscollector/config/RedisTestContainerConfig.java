package com.github.gluhov.iotplatform.eventscollector.config;

import org.testcontainers.containers.GenericContainer;

public class RedisTestContainerConfig {
    public static final int PORT = 6379;

    public static final GenericContainer<?> REDIS_CONTAINER = new GenericContainer<>("redis:8.6.7-alpine")
            .withExposedPorts(PORT);
}
