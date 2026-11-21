ALTER TABLE devices
    ADD COLUMN IF NOT EXISTS version    BIGINT NOT NULL DEFAULT 0, -- растёт на 1 при каждом APPLIED; по нему работает кэш
    ADD COLUMN IF NOT EXISTS managed_at TIMESTAMPTZ,               -- время последней применённой команды администратора
    ADD COLUMN IF NOT EXISTS deleted_by TEXT;                      -- DEVICE (событие роутера) или ADMIN;
                                                                   -- NULL у удалённых до V3 = DEVICE
ALTER TABLE processed_device_events DROP CONSTRAINT IF EXISTS processed_device_events_outcome_check;
ALTER TABLE processed_device_events ADD CONSTRAINT processed_device_events_outcome_check
    CHECK (outcome IN ('APPLIED', 'STALE', 'IGNORED'));
-- Право чтения для device-service. Роль создаётся вне миграций (кластерный объект), в тестах её нет
DO $$ BEGIN
    IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'device_reader') THEN
        GRANT USAGE ON SCHEMA public TO device_reader;
        GRANT SELECT ON devices TO device_reader;
    END IF;
END $$;
