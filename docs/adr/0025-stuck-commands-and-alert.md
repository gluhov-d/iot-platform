# ADR-0025: Зависшие команды и оповещение на каждую

**Статус**: предложено, 2026-11-28 (на проверке дизайна). Спецификация — [specs/003-router-manager](../../specs/003-router-manager/spec.md).

## Контекст

Задание: команду в SENT дольше срока отдельный механизм переводит в FAILED; срок — переменная окружения; на каждую такую команду
срабатывает оповещение в Grafana («роутер мог не пережить команду»). Отдельно: что делать, если ответ с командой потерялся.

## Решение

- Проход в каждом экземпляре каждые `ROUTER_STUCK_SCAN_INTERVAL` (10 с): `UPDATE … SET status='FAILED', failure_reason='TIMEOUT'
  WHERE command_id IN (SELECT … status='SENT' AND sent_at < now() - ROUTER_COMMAND_SENT_TIMEOUT … FOR UPDATE SKIP LOCKED) RETURNING`;
  каждую команду переводит ровно один экземпляр (research.md R2), время — часы PostgreSQL.
- На каждую переведённую команду — строка лога `event = command_stuck` с `command_id`, `router_id`, `command_type`, `sent_at` и
  `+1` к `router_manager_commands_stuck_total{type}` (без `command_id`).
- Правило Grafana `RouterManagerCommandStuck` по Loki: `sum by (command_id) (count_over_time({service="router-manager"} | json |
  event="command_stuck" [5m])) > 0`, интервал группы 10 с — отдельный экземпляр оповещения на каждую команду с её `command_id`
  (R18: 3 события → 3 экземпляра за 2–4 с). Провижининг из репозитория; доставка — свой contact point (предположение R19, проверяет T037).
- Потерянный ответ: команда остаётся SENT и не выдаётся повторно; через срок — FAILED (`TIMEOUT`) и оповещение; решение о новой
  команде — за администратором. Поздний ответ роутера итог не меняет (`applied = false`, `router_manager_late_reports_total`).

## Рассмотренные варианты

- Оповещение по счётчику без `command_id`: одно оповещение на группу, а не на команду; с `command_id` в метке — высокая
  кардинальность метрик (конституция VI).
- Повторная выдача той же команды тому же роутеру в пределах срока: меньше ложных FAILED, но REBOOT и UPDATE_AGENT неидемпотентны
  на роутере, а задание требует «выдаётся один раз».
- Проход в одном экземпляре с выбором ведущего: лишний механизм; `SKIP LOCKED` делает параллельные проходы безопасными.

## Последствия

- Экземпляр оповещения живёт 5 минут после события (окно запроса), затем закрывается; история — в Grafana и Loki.
- Оповещения работают с профилем `observability` (Loki, Grafana).
