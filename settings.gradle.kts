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
// Реестр устройств: приём DeviceInfo из Kafka и запись через ShardingSphere-Proxy
include("services:device-collector-service")
findProject(":services:device-collector-service")?.name = "device-collector-service"
// Клиент device-service: DevicesApi и DTO из OpenAPI-контракта, публикуется в Nexus
include("services:device-service-client")
// Точка входа CRUD и чтения устройств: команды - в Kafka, чтение - из модели чтения
include("services:device-service")
