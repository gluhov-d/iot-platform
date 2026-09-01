CREATE TABLE IF NOT EXISTS devices (
    id           UUID PRIMARY KEY,
    external_id  TEXT        NOT NULL UNIQUE,
    name         TEXT        NOT NULL,
    model        TEXT,
    status       TEXT        NOT NULL DEFAULT 'UNKNOWN',
    last_seen_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_devices_status ON devices (status);
CREATE INDEX IF NOT EXISTS idx_devices_last_seen ON devices (last_seen_at DESC);

CREATE TABLE IF NOT EXISTS device_commands (
    id           UUID PRIMARY KEY,
    device_id    UUID        NOT NULL REFERENCES devices (id) ON DELETE CASCADE,
    command_type TEXT        NOT NULL,
    payload      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status       TEXT        NOT NULL DEFAULT 'NEW',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    executed_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_commands_device ON device_commands (device_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_commands_status ON device_commands (status);
