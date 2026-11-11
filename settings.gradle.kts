plugins {
    // Автоматически скачивает JDK 25 для toolchain, если его нет на машине
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "iot-platform"
include("services:events-collector-service")
// Avro-схемы платформы: avros/avro -> jar с классами, публикуется в Nexus
include("avros")
include("services:db-migrator")
findProject(":services:db-migrator")?.name = "db-migrator"
include("services:device-collector-service")
findProject(":services:device-collector-service")?.name = "device-collector-service"
// Реестр устройств: приём DeviceInfo из Kafka и запись через ShardingSphere-Proxy
include("services:device-collector-service")
