# syntax=docker/dockerfile:1.7
# Сборка из корня репозитория (нужны contracts/avro и корневые файлы Gradle):
#   docker build -f services/events-collector-service/Dockerfile -t iot-platform/events-collector-service:0.1.0 .

# ---------------------------------------------------------------- сборка
FROM eclipse-temurin:25.0.4.1_1-jdk-noble AS build
WORKDIR /workspace
# Многомодульная сборка: wrapper и settings.gradle.kts лежат в корне репозитория
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle gradle
COPY contracts/avro contracts/avro
COPY services/events-collector-service services/events-collector-service
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon :services:events-collector-service:bootJar -x test \
 && java -Djarmode=tools -jar services/events-collector-service/build/libs/events-collector-service.jar \
      extract --layers --launcher --destination /extracted

# ---------------------------------------------------------------- запуск \
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