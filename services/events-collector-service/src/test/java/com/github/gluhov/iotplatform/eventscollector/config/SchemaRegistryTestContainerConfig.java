package com.github.gluhov.iotplatform.eventscollector.config;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;

import static com.github.gluhov.iotplatform.eventscollector.config.KafkaTestContainerConfig.*;

public class SchemaRegistryTestContainerConfig {
    public static final int PORT = 8081;

    public static final GenericContainer<?> SCHEMA_REGISTRY_CONTAINER =
            new GenericContainer<>("confluentinc/cp-schema-registry:8.2.4")
                    .withNetwork(NETWORK)
                    .withExposedPorts(PORT)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + PORT)
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + KAFKA_INTERNAL)
                    .dependsOn(KAFKA_CONTAINER)
                    .waitingFor(Wait.forHttp("/subjects").forStatusCode(200)
                            .withStartupTimeout(Duration.ofMinutes(2)));

    public static String schemaRegistryUrl() {
        return "http://%s:%d".formatted(SCHEMA_REGISTRY_CONTAINER.getHost(), SCHEMA_REGISTRY_CONTAINER.getMappedPort(PORT));
    }
}
