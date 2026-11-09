#!/usr/bin/env bash
# Регистрация схем avros в Schema Registry из контейнера schema-register (одноразовая задача compose).
#
# Идемпотентна: схема уже в subject - ничего не делает; новая и совместимая со всеми версиями (FULL_TRANSITIVE) -
# регистрируется; несовместимая - ошибка, код 1. Сервисы после этого работают с auto.register.schemas=false.
# Subject - TopicNameStrategy: <тема>-value. Имена тем - те же переменные, что у kafka-init и сервисов.
set -euo pipefail

: "${SCHEMA_REGISTRY_URL:?SCHEMA_REGISTRY_URL не задан}"
EVENTS_TOPIC="${KAFKA_EVENTS_TOPIC:?KAFKA_EVENTS_TOPIC не задан}"
DEVICES_TOPIC="${KAFKA_DEVICES_TOPIC:?KAFKA_DEVICES_TOPIC не задан}"
EVENTS_DLT_TOPIC="${KAFKA_EVENTS_DLT_TOPIC:?KAFKA_EVENTS_DLT_TOPIC не задан}"
DEVICES_DLT_TOPIC="${KAFKA_DEVICES_DLT_TOPIC:?KAFKA_DEVICES_DLT_TOPIC не задан}"
COMPATIBILITY="${SCHEMA_COMPATIBILITY:-FULL_TRANSITIVE}"

# events и events-dlt - DeviceEvent; devices и devices-dlt - DeviceInfo.
# В DLT-темы попадают и нечитаемые байты: они схемой не описываются
subjects="${EVENTS_TOPIC}-value=DeviceEvent,${EVENTS_DLT_TOPIC}-value=DeviceEvent,${DEVICES_TOPIC}-value=DeviceInfo,${DEVICES_DLT_TOPIC}-value=DeviceInfo"

cd /workspace
echo "==> Schema Registry ${SCHEMA_REGISTRY_URL}, совместимость ${COMPATIBILITY}"
./gradlew -q --offline --no-daemon --console=plain :avros:registerSchemas \
  -PschemaRegistryUrl="${SCHEMA_REGISTRY_URL}" \
  -PschemaSubjects="${subjects}" \
  -PschemaCompatibility="${COMPATIBILITY}"
echo "==> схемы avros зарегистрированы"
