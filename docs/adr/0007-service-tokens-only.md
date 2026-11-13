# ADR-0007: Только сервисные токены Keycloak

**Статус**: предложено, 2026-11-11 · **Спецификация**: [specs/001-device-service](../../specs/001-device-service/spec.md)

## Контекст

device-service вызывают сервисы (API-оркестратор), а не люди. Пользовательский токен заканчивается на оркестраторе.
Нулевое доверие: положение в сети ничего не доказывает. Keycloak сейчас в профиле `platform` с PostgreSQL
платформы, realm не настроен.

## Решение

- Realm `iot-platform` — файл в `infrastructure/config/keycloak/`, импортируется при старте (`--import-realm`);
  секреты клиентов подставляются из окружения. Keycloak — в базовом стенде, хранилище `dev-file`.
- Клиенты: `device-service` (роли `devices.read`, `devices.write`), `device-service-consumer` (client credentials,
  обе роли, audience mapper `device-service`), `device-service-reader` (только чтение). Пользователей в realm нет:
  тесты создают их через Admin API.
- Сервис — OAuth2 resource server: подпись по JWKS (ключи кэшируются, новый `kid` подтягивается), `iss`, `exp`/`nbf`,
  `aud = device-service`, токен выдан сервисной учётной записи (в токене есть `client_id` и нет `sid` — у
  пользовательского наоборот, research.md R8); роли из `resource_access.device-service.roles`.
- Токен, полученный с хоста, и токен, полученный внутри сети Compose, имеют один `iss` (`KC_HOSTNAME`); ключи сервис
  берёт по внутреннему адресу Keycloak.
- Пользовательский токен — `403 SERVICE_TOKEN_REQUIRED` даже с ролью и аудиторией; 401/403 — Problem Details.

## Последствия

- Стенд воспроизводим: realm в Git. Пересоздание контейнера Keycloak импортирует realm заново с новыми ключами — выданные токены перестают действовать;
  перезапуск контейнера ключи сохраняет.
- Отзыв токена — только истечением срока: срок жизни access token короткий (5 мин).

## Рассмотренные варианты

Проверка только роли (пользователь с той же ролью прошёл бы); introspection на каждый запрос (лишний сетевой
вызов); Keycloak в профиле `platform` (сервис не стартует без профиля).
