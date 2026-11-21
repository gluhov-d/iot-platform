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
# Срок хранения сообщений темы devices (FR-022, ADR-0011): тема не сжимается, сообщения ключа лежат целиком
# и в исходном порядке, пока не выйдут за срок; значение задаёт .env.example
: "${KAFKA_DEVICES_RETENTION_MS:?KAFKA_DEVICES_RETENTION_MS не задан}"
DEVICES_RETENTION_MS="${KAFKA_DEVICES_RETENTION_MS}"
# Единая тема необработанных сообщений (specs/002-failed-events): имя и срок хранения задаёт .env.example
: "${KAFKA_FAILED_EVENTS_TOPIC:?KAFKA_FAILED_EVENTS_TOPIC не задан}"
: "${KAFKA_FAILED_EVENTS_RETENTION_MS:?KAFKA_FAILED_EVENTS_RETENTION_MS не задан}"
FAILED_EVENTS_TOPIC="${KAFKA_FAILED_EVENTS_TOPIC}"
FAILED_EVENTS_RETENTION_MS="${KAFKA_FAILED_EVENTS_RETENTION_MS}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"

topics() { "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${KAFKA_BOOTSTRAP}" "$@"; }
configs() { "${KAFKA_BIN}/kafka-configs.sh" --bootstrap-server "${KAFKA_BOOTSTRAP}" "$@"; }

echo "==> ожидание брокера ${KAFKA_BOOTSTRAP}"
for _ in $(seq 1 30); do
  topics --list >/dev/null 2>&1 && break
  sleep 2
done
topics --list >/dev/null

create() {
  local name="$1" policy="$2" retention="${3:-}"
  echo "==> тема ${name}: partitions=${PARTITIONS}, rf=${REPLICATION}, cleanup.policy=${policy}"
  if [[ -n "${retention}" ]]; then
    topics --create --if-not-exists --topic "${name}" \
      --partitions "${PARTITIONS}" --replication-factor "${REPLICATION}" \
      --config "cleanup.policy=${policy}" --config "retention.ms=${retention}"
  else
    topics --create --if-not-exists --topic "${name}" \
      --partitions "${PARTITIONS}" --replication-factor "${REPLICATION}" \
      --config "cleanup.policy=${policy}"
  fi
}

# Сырой поток событий устройств: старые сообщения удаляются по retention
create "${EVENTS_TOPIC}" delete
# Поток устройств: команды администратора и события роутера с ключом deviceId. Сжатие выключено (ADR-0011) - оно
# оставляло на ключ последнее сообщение и стирало порядок: команда DELETE исчезала, а снимок роутера оставался
create "${DEVICES_TOPIC}" delete "${DEVICES_RETENTION_MS}"
# Уже существующая тема: --if-not-exists не меняет её настройки, поэтому политика и срок задаются явно
# (повторно - без изменений). Настройка сжатия снимается: с cleanup.policy=delete она не действует и только путает
echo "==> тема ${DEVICES_TOPIC}: cleanup.policy=delete, retention.ms=${DEVICES_RETENTION_MS}"
configs --entity-type topics --entity-name "${DEVICES_TOPIC}" --alter \
  --add-config "cleanup.policy=delete,retention.ms=${DEVICES_RETENTION_MS}"
configs --entity-type topics --entity-name "${DEVICES_TOPIC}" --alter \
  --delete-config "min.compaction.lag.ms" 2>/dev/null \
  || echo "    (min.compaction.lag.ms у темы не задан - снимать нечего)"
# Единая тема необработанных сообщений обоих сборщиков (сообщения, которые нельзя обработать или записать после повторов).
# Сжатия нет, срок задаёт KAFKA_FAILED_EVENTS_RETENTION_MS; retention.bytes не задаётся. Число разделов как у events.
# Уже существующая тема: --if-not-exists не меняет её настройки, поэтому политика и срок задаются явно (повторно - без изменений)
create "${FAILED_EVENTS_TOPIC}" delete "${FAILED_EVENTS_RETENTION_MS}"
configs --entity-type topics --entity-name "${FAILED_EVENTS_TOPIC}" --alter \
  --add-config "cleanup.policy=delete,retention.ms=${FAILED_EVENTS_RETENTION_MS}"

# Прежние темы необработанных сообщений (до specs/002-failed-events, темы events-dlt и devices-dlt) удаляются вместе
# с сообщениями. Имена заданы константами здесь, а не берутся из окружения, чтобы удаление не зависело от переменных.
# --if-exists: отсутствие темы - не ошибка, повторный запуск безопасен. На стенде владельца удаление необратимо.
topics --delete --if-exists --topic "events-dlt"
topics --delete --if-exists --topic "devices-dlt"

echo "==> итоговая конфигурация:"
topics --describe --topic "${EVENTS_TOPIC}"
topics --describe --topic "${DEVICES_TOPIC}"
configs --entity-type topics --entity-name "${DEVICES_TOPIC}" --describe
topics --describe --topic "${FAILED_EVENTS_TOPIC}"
