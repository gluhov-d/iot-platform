# ADR-0002: Поле operation в DeviceInfo

**Статус**: предложено, 2026-11-11 · **Спецификация**: [specs/001-device-service](../../specs/001-device-service/spec.md)

## Контекст

Задание: необязательное `operation` (`CREATE`, `UPDATE`, `DELETE`), default `null`, «null — создание», FULL.
В нашем `DeviceInfo` с версии 1.1.0 уже есть обязательное `eventType` (`CREATED`, `UPDATED`, `DELETED`, `SNAPSHOT`),
по которому device-collector-service применяет события роутера. В примере теории у enum default `CREATE`.

## Решение

- avros 1.3.0: `operation: ["null", OperationType]`, `default: null`; `OperationType` — `CREATE`, `UPDATE`, `DELETE`,
  `UNKNOWN`, default символа `UNKNOWN`.
- `null` — сообщение не команда, а событие роутера: применяется по `eventType`, как раньше. Для нового устройства
  это и есть создание, поэтому все старые сообщения и сообщения events-collector-service читаются без изменений.
- `UNKNOWN` (символ новее схемы читателя) — сообщение сразу в `devices-dlt`.
- device-service заполняет и `operation`, и согласованный `eventType` (`CREATED`/`UPDATED`/`DELETED`): поле
  обязательное, и читатели старых версий понимают сообщение.

## Последствия

- Совместимость FULL: проверяют `checkAvrosCompatibility` (Nexus) и `checkRegisteredSchemas` (Schema Registry).
- Отличие от буквы задания: `null` означает «событие роутера», а не «создание». Для роутера это одно и то же
  (вставка, если устройства нет), но команда администратора и событие обрабатываются по разным правилам (ADR-0005).

## Рассмотренные варианты

Default enum `CREATE`, как в теории: новая операция у старого потребителя молча стала бы созданием. Новая тема для
команд: два источника порядка для одного ключа, порядок между командой и событием не гарантирован.
