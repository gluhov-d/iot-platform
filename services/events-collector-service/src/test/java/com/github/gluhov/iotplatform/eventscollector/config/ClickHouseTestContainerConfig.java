package com.github.gluhov.iotplatform.eventscollector.config;

import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.utility.MountableFile;

import static com.github.gluhov.iotplatform.eventscollector.config.KafkaTestContainerConfig.REPO_ROOT;

public class ClickHouseTestContainerConfig {
    public static final String DATABASE = "iot_platform";
    public static final int HTTP_PORT = 8123;

    // Схему создает тот же 01-schema.sh, что и на локальном стенде; база - из CLICKHOUSE_DB
    public static final ClickHouseContainer CLICK_HOUSE_CONTAINER =
            new ClickHouseContainer("clickhouse/clickhouse-server:26.2.19-alpine")
                    .withDatabaseName(DATABASE)
                    .withCreateContainerCmdModifier(cmd -> cmd.withPlatform(platform()))
                    .withCopyFileToContainer(
                            MountableFile.forHostPath(REPO_ROOT.resolve("infrastructure/init/clickhouse")),
                            "/docker-entrypoint-initdb.d/");
    
    public static String jdbcUrl() {
        return "jdbc:clickhouse://%s:%d".formatted(CLICK_HOUSE_CONTAINER.getHost(), CLICK_HOUSE_CONTAINER.getMappedPort(HTTP_PORT));
    }

    // На Apple Silicon arm64-сборка ClickHouse 26.2 падает с SIGILL на SELECT ... FINAL, amd64 через Rosetta работает
    private static String platform() {
        String configured = System.getenv("CLICKHOUSE_PLATFORM");
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        boolean arm = "aarch64".equals(System.getProperty("os.arch"));
        boolean mac = System.getProperty("os.name", "").startsWith("Mac");
        return arm && !mac ? "linux/arm64": "linux/amd64";
    }
}
