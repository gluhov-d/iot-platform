# syntax=docker/dockerfile:1.7
# Образ публикации avros: собирает Avro-схемы платформы (генерация классов, проверка) и кладёт версию
# в Nexus. Запускается одноразовой задачей compose avros-publish; сборка из корня репозитория
# (нужны корневые файлы Gradle):
#   docker build -f avros/Dockerfile -t iot-platform/avros-publish:0.1.0 .

FROM eclipse-temurin:25.0.4.1_1-jdk-noble AS publisher
# curl - для проверки версии в Nexus скриптом публикации
RUN apt-get update && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system app && useradd --system --gid app --create-home app
WORKDIR /workspace
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle gradle
# settings.gradle.kts подключает все модули: для конфигурации нужны только их build-файлы
COPY services/events-collector-service/build.gradle.kts services/events-collector-service/
COPY services/device-collector-service/build.gradle.kts services/device-collector-service/
COPY services/db-migrator/build.gradle.kts services/db-migrator/
COPY avros avros
# Сборка при создании образа. Кеш /root/.gradle (дистрибутив Gradle, зависимости) общий с образами сервисов;
# после сборки он копируется в образ - зависимости сборки в образе
RUN --mount=type=cache,target=/root/.gradle \
    ./gradlew --no-daemon :avros:build -x test \
 && mkdir -p /home/app/.gradle && cp -a /root/.gradle/. /home/app/.gradle/ \
 && chown -R app:app /workspace /home/app/.gradle
USER app
# Дальше Gradle работает под app с кешем из образа
ENV GRADLE_USER_HOME=/home/app/.gradle
ENTRYPOINT ["/workspace/avros/scripts/container-publish.sh"]
