-- Записывает карточку устройства в кэш, если её версия не ниже порога floor и не ниже уже закэшированной.
-- KEYS[1] = device:cache:v1:{deviceId} (hash: body, version) - срок жизни DEVICE_CACHE_TTL;
-- KEYS[2] = device:floor:v1:{deviceId} - порог, его пишет device-collector-service после COMMIT. У порога свой,
--           более долгий срок (DEVICE_CACHE_FLOOR_TTL): в одном ключе с ответом он истекал вместе с ним, и отставание
--           реплики дольше TTL снова возвращало старые данные в кэш (ADR-0003).
-- ARGV[1] = body (JSON), ARGV[2] = версия записи, ARGV[3] = TTL кэша, мс.
-- Возврат: 1 - записано и ключ порога удалён (реплика догнала применённую версию);
--          0 - отказ (версия реплики устарела): ключи не меняются, TTL кэша не продлевается.
local body = ARGV[1]
local version = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local floor = tonumber(redis.call('GET', KEYS[2]) or '0')
if version < floor then
  return 0
end
local cached = tonumber(redis.call('HGET', KEYS[1], 'version') or '-1')
if version < cached then
  return 0
end
redis.call('HSET', KEYS[1], 'body', body, 'version', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ttl)
redis.call('DEL', KEYS[2])
return 1
