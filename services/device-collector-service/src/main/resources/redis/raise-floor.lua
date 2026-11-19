-- Поднимает порог версии устройства (floor) и убирает из кэша устаревший ответ.
-- KEYS[1] = device:cache:v1:{deviceId} (hash: body, version) - кэш device-service, его срок не меняем;
-- KEYS[2] = device:floor:v1:{deviceId} - порог; живёт дольше кэша (DEVICE_CACHE_FLOOR_TTL), пока чтение не положит
--           в кэш версию не ниже порога (тогда device-service удаляет этот ключ). В одном ключе с ответом порог
--           истекал вместе с ним, и отставание реплики дольше TTL возвращало старые данные (ADR-0003).
-- ARGV[1] = версия записи после APPLIED; ARGV[2] = срок жизни порога, мс.
-- floor только растёт. Если закэшированная версия ниже порога - body и version удаляются (промах при следующем GET).
-- Отсутствие ключей и полей - не ошибка: порог создаётся, срок ставится только ему.
local version = tonumber(ARGV[1])
local floorTtl = tonumber(ARGV[2])
local floor = tonumber(redis.call('GET', KEYS[2]) or '0')
if version > floor then
  floor = version
  redis.call('SET', KEYS[2], floor)
end
redis.call('PEXPIRE', KEYS[2], floorTtl)
local cached = tonumber(redis.call('HGET', KEYS[1], 'version') or '-1')
if cached >= 0 and cached < floor then
  redis.call('HDEL', KEYS[1], 'body', 'version')
end
return floor
