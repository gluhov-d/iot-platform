#!/bin/bash
# Запускается одноразовым контейнером kafka-init после того, как kafka стала healthy.
# Идемпотентен: существующие темы не пересоздаются (--if-not-exists), поэтому повторный запуск безопасен.
#
# --if-not-exists не меняет настройки уже существующей темы. Если тема была создана с другой
# cleanup.policy, исправьте ее через kafka-configs.sh --alter или пересоздайте том Kafka (make kafka-reset).

set -euo pipefail

: "${KAFKA_BOOTSTRAP:?KAFKA_BOOTSTRAP не задан}"
EVENTS_TOPIC="${KAFKA_EVENTS_TOPIC:-events}"
DEVICES_TOPIC="${KAFKA_DEVICES_TOPIC:-devices}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
REPLICATION="${KAFKA_TOPIC_REPLICATION:-1}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"

topics() { "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${KAFKA_BOOTSTRAP}" "$@"; }

echo "==> ожидание брокера ${KAFKA_BOOTSTRAP}"
for _ in $(seq 1 30); do
  topics --list >/dev/null 2>&1 && break
  sleep 2
done
topics --list >/dev/null

create() {
  local name="$1" policy="$2"
  echo "==> тема ${name}: partitions=${PARTITIONS}, rf=${REPLICATION}, cleanup.policy=${policy}"
  topics --create --if-not-exists --topic "${name}" \
    --partitions "${PARTITIONS}" --replication-factor "${REPLICATION}" \
    --config "cleanup.policy=${policy}"
}

# Сырой поток событий устройств: старые сообщения удаляются по retention
create "${EVENTS_TOPIC}" delete
# Производный поток устройств: важен последний факт по ключу deviceId
create "${DEVICES_TOPIC}" compact

echo "==> итоговая конфигурация:"
topics --describe --topic "${EVENTS_TOPIC}"
topics --describe --topic "${DEVICES_TOPIC}"
