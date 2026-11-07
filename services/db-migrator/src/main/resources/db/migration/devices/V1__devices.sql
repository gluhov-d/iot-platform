-- Реестр устройств. Одинаковая схема на каждом шарде: db-migrator применяет её к шардам напрямую,
-- ShardingSphere-Proxy маршрутизирует запросы по device_id (devices и processed_device_events - связанные таблицы,
-- запись устройства и его журнал всегда на одном шарде, транзакция локальная).
-- Запросов без device_id нет по условию задания, поэтому вторичных индексов нет.

-- Текущее состояние устройства: последний применённый снимок + счётчики событий
CREATE TABLE devices (
                         device_id        TEXT        PRIMARY KEY,               -- ключ шардирования
                         tenant_id        TEXT,                                  -- поля снимка; NULL, пока снимка не было
                         owner_id         TEXT,
                         device_type      TEXT,
                         serial_number    TEXT,
                         manufacturer     TEXT,
                         model            TEXT,
                         firmware_version TEXT,
                         status           TEXT        NOT NULL DEFAULT 'UNKNOWN',
                         registered_at    TIMESTAMPTZ,
                         last_seen_at     TIMESTAMPTZ,
                         settings         JSONB       NOT NULL DEFAULT '{}',
                         attributes       JSONB       NOT NULL DEFAULT '{}',
                         sensors          JSONB       NOT NULL DEFAULT '[]',     -- снимок целиком: сенсоры приходят и пишутся вместе с устройством
                         seen_count       BIGINT      NOT NULL CHECK (seen_count > 0), -- различные события устройства (APPLIED + STALE), повтор не считается
                         first_seen_at    TIMESTAMPTZ NOT NULL,
                         last_event_at    TIMESTAMPTZ NOT NULL,                  -- самое позднее событие устройства (любого типа)
                         last_event_id    TEXT,
                         last_event_type  TEXT        NOT NULL,
                         last_state_at    TIMESTAMPTZ,                           -- событие, давшее текущее состояние (снимок или удаление);
    -- NULL - состояния ещё не было. По нему отсекаются устаревшие снимки
                         deleted_at       TIMESTAMPTZ,                           -- мягкое удаление: событие DELETED
                         created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
                         updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Журнал обработанных событий: защита от повторной доставки. Первичный ключ (device_id, event_key) на том же шарде,
-- что и устройство; INSERT ... ON CONFLICT DO NOTHING вернул 0 строк - событие уже обработано
CREATE TABLE processed_device_events (
                                         device_id       TEXT        NOT NULL,
                                         event_key       TEXT        NOT NULL,                   -- DeviceInfo.eventId; для сообщений 1.0.0 - 'emitted:<emittedAtMs>'
                                         event_type      TEXT        NOT NULL,
                                         event_at        TIMESTAMPTZ NOT NULL,
                                         emitted_at      TIMESTAMPTZ NOT NULL,
                                         outcome         TEXT        NOT NULL CHECK (outcome IN ('APPLIED', 'STALE')),
                                         kafka_topic     TEXT        NOT NULL,
                                         kafka_partition INT         NOT NULL,
                                         kafka_offset    BIGINT      NOT NULL,
                                         processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
                                         PRIMARY KEY (device_id, event_key)
);