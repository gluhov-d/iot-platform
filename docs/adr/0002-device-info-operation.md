# ADR-0002: Поле operation в DeviceInfo

**Статус**: предложено, 2026-11-11 · **Спецификация**: [specs/001-device-service](../../specs/001-device-service/spec.md)

## Контекст

Задание: необязательное `operation` (`CREATE`, `UPDATE`, `DELETE`), default `null`, «null — создание», FULL.
В нашем `DeviceInfo` с версии 1.1.0 уже есть обязательное `eventType` (`CREATED`, `UPDATED`, `DELETED`, `SNAPSHOT`),
по которому device-collector-service применяет события роутера. В примере теории у enum default `CREATE`.

## Решение

- avros 1.3.0: `operation: ["null", OperationType]`, `default: null`; `OperationType` — `CREATE`, `UPDATE`, `DELETE`,
  `UNKNOWN`, default символа `UNKNOWN`.
- `null` — поле не заполнено: так читаются сообщения прежних версий схемы и события events-collector-service. Такое
  сообщение применяется по `eventType`, как раньше; для нового устройства это и есть создание.
- `UNKNOWN` нужен для обратного случая: читатель со схемой 1.3.0 получает сообщение с операцией, которой в его схеме
  ещё нет, и Avro подставляет символ по умолчанию. Такое сообщение уходит в `devices-dlt`, а не применяется.
- device-service заполняет и `operation`, и согласованный `eventType` (`CREATED`/`UPDATED`/`DELETED`): поле
  обязательное, и читатели старых версий понимают сообщение.

## Последствия

- Совместимость FULL: проверяют `checkAvrosCompatibility` (Nexus) и `checkRegisteredSchemas` (Schema Registry).
- Уточнение к заданию («null — создание»): для сообщения без `operation` создание выполняется, если устройства ещё
  нет; если оно есть — действует прежняя логика `eventType`. Команды и события применяются по разным правилам (ADR-0005).

## Рассмотренные варианты

Default enum `CREATE`, как в теории: новая операция у старого потребителя молча стала бы созданием. Новая тема для
команд: два источника порядка для одного ключа, порядок между командой и событием не гарантирован.
