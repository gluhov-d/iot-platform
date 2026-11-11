-- Имя устройства, операционная система и её версия (avros 1.2.0, DeviceSnapshot.name / osName / osVersion).
-- Обратно совместимо: колонки необязательные, без значений по умолчанию (NULL - снимок без этих данных:
-- устройство ещё не прислало их или продюсер версии 1.1.0). firmware_version есть с V1.
-- IF NOT EXISTS: повторный запуск на шарде, где колонки уже есть, ничего не меняет.
ALTER TABLE devices
    ADD COLUMN IF NOT EXISTS name       TEXT,
    ADD COLUMN IF NOT EXISTS os_name    TEXT,
    ADD COLUMN IF NOT EXISTS os_version TEXT;