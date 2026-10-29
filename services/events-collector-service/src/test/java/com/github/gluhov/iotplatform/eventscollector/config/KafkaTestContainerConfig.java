package com.github.gluhov.iotplatform.eventscollector.config;

import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Path;

public class KafkaTestContainerConfig {
    public static final Path REPO_ROOT = Path.of("../..").toAbsolutePath().normalize();
    public static final Network NETWORK = Network.newNetwork();
    public static final String KAFKA_INTERNAL = "kafka:19092";

    // Темы создает тот же init-topics.sh, что и kafka-init на локальном стенде
    public static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer("apache/kafka:4.2.0")
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka")
            .withListener(KAFKA_INTERNAL)
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
            .withCopyFileToContainer(
                    MountableFile.forHostPath(REPO_ROOT.resolve("infrastructure/init/kafka/init-topics.sh"), 0755),
                            "/opt/init/init-topics.sh");
}
