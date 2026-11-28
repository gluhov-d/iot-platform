# Data Model: router-manager

## PostgreSQL: база `iot_commands`, таблица `router_commands`

Миграция `services/db-migrator/src/main/resources/db/migration/router-manager/V1__router_commands.sql`
(`IF NOT EXISTS`, ADR-0026). Роль `router_manager` и база создаются заранее задачей `router-manager-db-init` (research.md R22).

```sql
CREATE TABLE IF NOT EXISTS router_commands (
    command_id     uuid        PRIMARY KEY,                 -- UUIDv7 из сервиса: упорядочен по времени
    router_id      text        NOT NULL,
    command_type   text        NOT NULL CHECK (command_type IN ('REBOOT', 'UPDATE_AGENT')),
    params         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    request_id     text        NOT NULL,                    -- ключ идемпотентности; пустой у вызывающего → = command_id
    status         text        NOT NULL CHECK (status IN ('NEW', 'SENT', 'SUCCESS', 'FAILED')),
    failure_reason text        NULL CHECK (failure_reason IN ('ROUTER_FAILED', 'TIMEOUT')),
    result_message text        NULL,                        -- текст роутера, до 1024 символов
    created_by     text        NOT NULL,                    -- client_id сервисного токена
    created_at     timestamptz NOT NULL DEFAULT now(),
    sent_at        timestamptz NULL,
    finished_at    timestamptz NULL,
    CONSTRAINT router_commands_request_uq UNIQUE (router_id, request_id),
    CONSTRAINT router_commands_failed_reason_ck CHECK ((status = 'FAILED') = (failure_reason IS NOT NULL))
);

-- Выдача: самая старая NEW роутера (частичный индекс — только ожидающие)
CREATE INDEX IF NOT EXISTS router_commands_new_ix ON router_commands (router_id, created_at, command_id) WHERE status = 'NEW';
-- Проход зависших: SENT по времени выдачи
CREATE INDEX IF NOT EXISTS router_commands_sent_ix ON router_commands (sent_at) WHERE status = 'SENT';

GRANT SELECT, INSERT, UPDATE ON router_commands TO router_manager;
```

Время — `now()` PostgreSQL, а не часы экземпляра: у двух экземпляров одни часы.

### Выражения

| Операция | SQL (суть) |
|---|---|
| Создание | `INSERT … ON CONFLICT (router_id, request_id) DO NOTHING RETURNING *`; пусто (конфликт) → `SELECT` существующей по `(router_id, request_id)` и её возврат; поля повторного вызова не сравниваются — `request_id` означает «тот же вызов» |
| Выдача | `UPDATE router_commands c SET status='SENT', sent_at=now() WHERE c.command_id = (SELECT command_id FROM router_commands WHERE router_id=$1 AND status='NEW' ORDER BY created_at, command_id LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING c.*, EXISTS (SELECT 1 FROM router_commands o WHERE o.router_id=$1 AND o.status='NEW' AND o.command_id <> c.command_id) AS more_pending` (форма из research.md R1) |
| Ответ | `UPDATE … SET status=$3, failure_reason=$4, result_message=$5, finished_at=now() WHERE command_id=$1 AND router_id=$2 AND status='SENT' RETURNING status`; 0 строк → `SELECT status` для ответа и кода ошибки |
| Зависшие | `UPDATE … SET status='FAILED', failure_reason='TIMEOUT', finished_at=now() WHERE command_id IN (SELECT command_id FROM router_commands WHERE status='SENT' AND sent_at < now() - $1::interval ORDER BY sent_at LIMIT $2 FOR UPDATE SKIP LOCKED) RETURNING command_id, router_id, command_type, sent_at` |
| Снимок статусов | `SELECT status, count(*) FROM router_commands GROUP BY status` раз в `ROUTER_STATUS_SNAPSHOT_INTERVAL` |

## Переходы статусов

| Статус | Выдача (`PollCommand`) | Ответ SUCCESS | Ответ FAILED | Проход зависших |
|---|---|---|---|---|
| NEW | → SENT, `sent_at` | `FAILED_PRECONDITION` | `FAILED_PRECONDITION` | — |
| SENT | — (не выдаётся повторно) | → SUCCESS, `applied=true` | → FAILED `ROUTER_FAILED`, `applied=true` | старше срока → FAILED `TIMEOUT` |
| SUCCESS | — | без изменений, `applied=false` | без изменений, `applied=false` | — |
| FAILED | — | без изменений, `applied=false`, `late_reports_total+1` если `TIMEOUT` | то же | — |

## Проверка входных данных

| Поле | Правило | Ошибка |
|---|---|---|
| `router_id` (создание; метаданные роутера) | `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$` | `INVALID_ARGUMENT` / `UNAUTHENTICATED` |
| `type` | `REBOOT` или `UPDATE_AGENT` | `INVALID_ARGUMENT` |
| `params` | REBOOT — пусто; UPDATE_AGENT — только `version`, `^[0-9A-Za-z.+-]{1,32}$` | `INVALID_ARGUMENT` |
| `request_id` | пусто или до 128 символов `[A-Za-z0-9._:-]` | `INVALID_ARGUMENT` |
| `command_id` | UUID | `INVALID_ARGUMENT` |
| `message` (ответ) | до 1024 символов UTF-8 | `INVALID_ARGUMENT` |

## Redis (общий Redis платформы через Sentinel)

| Ключ | Значение | Срок | Кто пишет |
|---|---|---|---|
| `router:cmd:none:v1:{routerId}` | `1` — «команды нет» | `ROUTER_NEG_CACHE_TTL` (15 с) | опрос, только если поколение не менялось (Lua) |
| `router:cmd:gen:v1:{routerId}` | счётчик поколения | `ROUTER_GEN_TTL` (1 ч), продлевается при создании | создание после фиксации (Lua: `INCR`, `EXPIRE`, `DEL none`) |
| `router:sig:v1:{routerId}:{sig}` | `1` — подпись уже принята | `ROUTER_AUTH_REPLAY_TTL` (120 с) | перехватчик подписи после проверки HMAC: `SET NX PX`; не записалось — повтор, отказ |

Lua записи «нет команды» (KEYS[1] = none, KEYS[2] = gen, ARGV[1] = поколение до запроса или пустая строка, ARGV[2] = срок, мс):
`local g = redis.call('GET', KEYS[2]) or '' if g == ARGV[1] then redis.call('SET', KEYS[1], '1', 'PX', ARGV[2]) return 1 end return 0`.

Граница устаревания (ADR-0022): запись «нет команды», появившаяся до создания или во время вызова, начатого до него,
живёт не дольше `ROUTER_REQUEST_TIMEOUT + ROUTER_NEG_CACHE_TTL` = 2 + 15 = 17 с после фиксации; третий опрос при интервале 10 с —
не раньше чем через 20 с, поэтому идёт в базу. Сервис отказывается стартовать при `TTL + REQUEST_TIMEOUT ≥ 20 с`.

## Метаданные запроса роутера (ADR-0023)

| Ключ | Значение |
|---|---|
| `x-router-id` | идентификатор роутера |
| `x-router-ts` | время Unix, секунды, десятичное без ведущих нулей |
| `x-router-nonce` | 16 случайных байт, `base64url` без `=`; новый на каждый запрос (два опроса в одну секунду различаются) |
| `x-router-key-id` | идентификатор мастер-ключа (`ROUTER_AUTH_KEYS`) |
| `x-router-sig` | `base64url` без `=` от `HMAC-SHA256(K_r, строка)`, `K_r = HMAC-SHA256(K_master, router_id)` |

Строка подписи: `v1\n` + полное имя метода + `\n` + router_id + `\n` + ts + `\n` + nonce + `\n` + command_id + `\n` + outcome + `\n` + hex(SHA-256(message)). Полное имя метода — `/iotplatform.router.v1.RouterAgentService/PollCommand`; у `PollCommand`
`command_id` и `outcome` пустые, `message` пустое (SHA-256 пустой строки); `outcome` — имя значения enum (`COMMAND_OUTCOME_SUCCESS`).
Порядок проверки: метаданные → ключ по `key-id` → окно времени → `hmac.Equal` → `SET NX` (в Redis пишутся только верные подписи,
подбором его не заполнить).

## Строка лога зависшей команды (источник оповещения)

`{"level":"WARN","msg":"command stuck","event":"command_stuck","command_id":"…","router_id":"…","command_type":"REBOOT","sent_at":"…","traceId":"…"}`
