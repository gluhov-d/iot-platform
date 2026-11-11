# Research: device-service

Что предполагается о стороннем ПО и как проверяется до кода (фаза 0 `tasks.md`, T001–T010). У каждого пункта —
запасной вариант; если сработал запасной, правится plan.md и соответствующий ADR.

## Уже проверено при подготовке дизайна

| Вопрос | Результат |
|---|---|
| Контракт валиден | `openapi-generator-cli:v7.17.0 validate` — `No validation issues detected` |
| `spring` + `library spring-http-interface` даёт Bean Validation, как в примере теории | **Нет.** Генератор 7.17.0 пишет: «For Spring HTTP Interface following options are disabled: … useBeanValidation, performBeanValidation». Интерфейс `DevicesApi` (`@HttpExchange`, `@RequestBody`, `@PathVariable`) генерируется, модели — без ограничений |
| Как получить DTO с ограничениями | Два прогона генератора по одной спецификации: модели — генератор `spring` (`-Dmodels`, `useBeanValidation=true`) дают `@NotNull`, `@Size`, `@Pattern`; интерфейс — `spring-http-interface` без моделей (`-Dapis`, `generateModels=false`) |
| `pattern` из OpenAPI → `@Pattern` | OpenAPI ищет совпадение в строке, `@Pattern` требует совпадения всей строки. `pattern: '\S'` пропустил бы только односимвольные значения, поэтому в контракте шаблоны записаны целиком: `'^.*\S.*$'`, `'^[^/\s]+$'` |

## Spike (до кода)

| # | Вопрос | Если не работает |
|---|---|---|
| 1 | Схема двух прогонов генератора в Gradle (`org.openapi.generator` 7.17.0) компилируется на Spring Boot 4 / Framework 7; модели в Git не попадают | генерировать серверный интерфейс `spring` (`interfaceOnly`) для контроллера и отдельно клиент; задание «контроллер реализует интерфейс клиента» — отклонение в ADR-0009 |
| 2 | Контроллер, реализующий `DevicesApi`, получает маршруты Spring MVC из `@HttpExchange` и проверку тела (`@Valid` на параметре реализации) без `ConstraintDeclarationException` | `@Validated` на классе + явный `Validator` в сервисном слое |
| 3 | ShardingSphere-Proxy 5.5.3: вторая логическая база `devices_read` с источниками-репликами (hot standby) и пользователь, которому доступна только она (`privilege: DATABASE_PERMITTED`) | read-only пользователь Proxy без ограничения базы + `device_reader` на репликах (запись невозможна физически); крайний случай — второй экземпляр Proxy |
| 4 | Keycloak 26: `start-dev --import-realm`, подстановка секретов клиентов из окружения в файл realm, повторный старт без ошибок | одноразовая задача `keycloak-config-cli` после старта |
| 5 | Один `iss` для токенов с хоста и из сети Compose (`KC_HOSTNAME`), сервис берёт JWKS по внутреннему адресу | `issuer-uri` не задавать: `jwk-set-uri` + явный валидатор `iss` |
| 6 | Признак сервисной учётной записи в токене Keycloak 26 (`client_id`, `preferred_username = service-account-*`) | свой claim `token_type = service` через mapper только у клиентов-потребителей |
| 7 | Redis Sentinel в Compose: `resolve-hostnames`/`announce-hostnames`, Lettuce находит master, `EVAL` идёт на master, после failover сервисы работают без перезапуска | `announce-ip` статических адресов сети Compose |
| 8 | Полный путь в IT: Testcontainers `ComposeContainer` поднимает нужные сервисы платформенного Compose (Kafka, SR, шарды с репликами, db-migrator, Proxy, device-collector-service, Redis, Keycloak) за приемлемое время | IT device-service — до Kafka и с репликой без device-collector-service; полный путь — задача Gradle против поднятого стенда (`e2eTest`, как `quickCheck`) |
| 9 | Один traceId: observation у `KafkaTemplate` device-service и listener device-collector-service, `traceparent` в заголовке | явная передача контекста через `KafkaTemplate` interceptor |
| 10 | Память Mac: Keycloak (~600 МБ), 2 × device-service, Redis ×5 поверх текущего стенда | второй экземпляр device-service — профилем `scale`, по умолчанию один |
| 11 | `volatile-lru`: вытеснение `outbox:lease` при нехватке памяти не ломает публикацию events-collector-service (проверка `ownerToken`, `ItRedisPublishedDeviceStoreTest`) | `maxmemory` не задавать (как сейчас): вытеснения нет, объём кэша ограничен TTL |
