# syntax=docker/dockerfile:1.7
# Сборка из корня репозитория (нужны корневые файлы Gradle); Avro-контракты Gradle берёт из Nexus:
#   docker build -f services/failed-events-processor/Dockerfile \
#     --add-host host.docker.internal:host-gateway --build-arg NEXUS_URL=http://host.docker.internal:8091 \
#     -t iot-platform/failed-events-processor:0.1.0 .

# ---------------------------------------------------------------- сборка
FROM eclipse-temurin:25.0.4.1_1-jdk-noble AS build
# Nexus хоста: localhost внутри сборки - сам контейнер сборки
ARG NEXUS_URL=http://host.docker.internal:8091
WORKDIR /workspace
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle gradle
# settings.gradle.kts подключает все модули: для конфигурации нужны только их build-файлы
COPY avros/build.gradle.kts avros/
COPY services/events-collector-service/build.gradle.kts services/events-collector-service/
COPY services/db-migrator/build.gradle.kts services/db-migrator/
COPY services/device-collector-service/build.gradle.kts services/device-collector-service/
COPY services/device-service-client/build.gradle.kts services/device-service-client/
COPY services/device-service/build.gradle.kts services/device-service/
COPY services/failed-events-processor/build.gradle.kts services/failed-events-processor/
COPY services/failed-events-processor services/failed-events-processor
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon :services:failed-events-processor:bootJar -x test -PnexusUrl="${NEXUS_URL}" \
 && java -Djarmode=tools -jar services/failed-events-processor/build/libs/failed-events-processor.jar \
      extract --layers --launcher --destination /extracted

# ---------------------------------------------------------------- запуск
FROM eclipse-temurin:25.0.4.1_1-jre-noble
RUN groupadd --system app && useradd --system --gid app app
WORKDIR /app
# Слои Spring Boot: зависимости меняются реже кода и кешируются отдельно
COPY --from=build /extracted/dependencies/ ./
COPY --from=build /extracted/spring-boot-loader/ ./
COPY --from=build /extracted/snapshot-dependencies/ ./
COPY --from=build /extracted/application/ ./
USER app
EXPOSE 8080
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
