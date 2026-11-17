# ADR-0009: API First и клиентская библиотека

**Статус**: предложено, 2026-11-11 · **Спецификация**: [specs/001-device-service](../../specs/001-device-service/spec.md)

## Контекст

Контракт — файл OpenAPI в репозитории, спецификация из аннотаций не принимается. Из спецификации генерируются DTO и
интерфейс HTTP Service Client; контроллер реализует интерфейс; интерфейс и DTO публикуются в Nexus как
`device-service-client`. Проверка генератора 7.17.0 показала: с `library=spring-http-interface` опция
`useBeanValidation` отключается, модели получаются без ограничений.

## Решение

- Подпроект `services/device-service-client`, спецификация — `src/main/openapi/device-service.yaml` (версия
  библиотеки = `info.version`). Сгенерированное — в `build/`, не в Git.
- Два прогона генератора по одной спецификации: модели — генератор `spring` с `useBeanValidation`, интерфейс
  `DevicesApi` — `spring-http-interface` без моделей.
- device-service подключает `project(":services:device-service-client")`; `DeviceController implements DevicesApi`,
  тело запроса проверяется `@Valid`. Расхождение реализации с контрактом ломает компиляцию.
- Публикация в Nexus — `make ds-client-release`; выпущенная версия не перезаписывается (как avros).
- Шаблоны `pattern` записаны с якорями (`^…$`): OpenAPI ищет совпадение в строке, `@Pattern` — во всей строке.

## Последствия

- Потребитель подключает `device-service-client` нужной версии и создаёт прокси `DevicesApi` через
  `@ImportHttpServices`; транспорт ему неважен.
- Несовместимые изменения — `/api/v2` и мажорная версия библиотеки; v1 помечается `deprecated`.

## Рассмотренные варианты

Один прогон `spring-http-interface` (нет валидации); серверный интерфейс `spring` для контроллера и отдельный клиент
(контроллер не реализует интерфейс клиента — отклонение от задания; запасной вариант, research.md R2).
