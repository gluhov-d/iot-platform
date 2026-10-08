plugins {
    // Автоматически скачивает JDK 25 для toolchain, если его нет на машине
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "iot-platform"
include("services:events-collector-service")