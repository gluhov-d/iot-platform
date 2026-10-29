package com.github.gluhov.iotplatform.eventscollector.it;

import com.github.gluhov.iotplatform.eventscollector.config.ClickHouseTestContainerConfig;
import com.github.gluhov.iotplatform.eventscollector.config.SchemaRegistryTestContainerConfig;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;

import static com.github.gluhov.iotplatform.eventscollector.config.ClickHouseTestContainerConfig.CLICK_HOUSE_CONTAINER;
import static com.github.gluhov.iotplatform.eventscollector.config.ClickHouseTestContainerConfig.DATABASE;
import static com.github.gluhov.iotplatform.eventscollector.config.KafkaTestContainerConfig.KAFKA_CONTAINER;
import static com.github.gluhov.iotplatform.eventscollector.config.KafkaTestContainerConfig.KAFKA_INTERNAL;
import static com.github.gluhov.iotplatform.eventscollector.config.RedisTestContainerConfig.PORT;
import static com.github.gluhov.iotplatform.eventscollector.config.RedisTestContainerConfig.REDIS_CONTAINER;
import static com.github.gluhov.iotplatform.eventscollector.config.SchemaRegistryTestContainerConfig.SCHEMA_REGISTRY_CONTAINER;

@SpringBootTest
@ActiveProfiles({"test", "text"})
public abstract class AbstractItTest {
    protected static final KafkaContainer kafka = KAFKA_CONTAINER;
    protected static final GenericContainer<?> schemaRegistry = SCHEMA_REGISTRY_CONTAINER;
    protected static final ClickHouseContainer clickhouse = CLICK_HOUSE_CONTAINER;
    protected static final GenericContainer<?> redis = REDIS_CONTAINER;
    
    // Контейнеры поднимаются один раз на JVM и переиспользуются всеми интеграционными тестами
    static {
        Startables.deepStart(kafka, schemaRegistry, clickhouse, redis).join();
        createTopics();
    }
    
    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("KAFKA_BOOTSTRAP_SERVERS", kafka::getBootstrapServers);
        registry.add("KAFKA_CONSUMER_GROUP", () -> "events-collector-it");
        registry.add("SCHEMA_REGISTRY_URL", SchemaRegistryTestContainerConfig::schemaRegistryUrl);
        registry.add("CLICKHOUSE_URL", ClickHouseTestContainerConfig::jdbcUrl);
        registry.add("CLICKHOUSE_DATABASE", () -> DATABASE);
        registry.add("CLICKHOUSE_USERNAME", clickhouse::getUsername);
        registry.add("CLICKHOUSE_PASSWORD", clickhouse::getPassword);
        registry.add("REDIS_HOST", redis::getHost);
        registry.add("REDIS_PORT", () -> redis.getMappedPort(PORT));
    }

    private static void createTopics() {
        try {
            var result = kafka.execInContainer("bash", "-c",
                    "KAFKA_BOOTSTRAP=" + KAFKA_INTERNAL + " /opt/init/init-topics.sh");
            if (result.getExitCode() != 0) {
                throw new IllegalStateException("init-topics.sh exited with code " + result.getExitCode()
                        + ":\n" + result.getStdout() + result.getStderr());
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create Kafka topics", e);
        }
    }
}
