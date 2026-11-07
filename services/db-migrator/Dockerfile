# syntax=docker/dockerfile:1.7
# Сборка из корня репозитория (нужны корневые файлы Gradle):
#   docker build -f services/db-migrator/Dockerfile -t iot-platform/db-migrator:0.1.0 .

# ---------------------------------------------------------------- сборка
FROM eclipse-temurin:25.0.4.1_1-jdk-noble AS build
WORKDIR /workspace
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle gradle
# settings.gradle.kts подключает все модули: для конфигурации нужны только их build-файлы
COPY contracts/build.gradle.kts contracts/
COPY services/events-collector-service/build.gradle.kts services/events-collector-service/
COPY services/device-collector-service/build.gradle.kts services/device-collector-service/
COPY services/db-migrator services/db-migrator
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon :services:db-migrator:installDist -x test

# ---------------------------------------------------------------- запуск
FROM eclipse-temurin:25.0.4.1_1-jre-noble
RUN groupadd --system app && useradd --system --gid app app
WORKDIR /app
COPY --from=build /workspace/services/db-migrator/build/install/db-migrator/ ./
USER app
ENTRYPOINT ["/app/bin/db-migrator"]