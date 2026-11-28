# shellcheck shell=bash
# Subject'ы Schema Registry платформы (подключается через source, сама не запускается).
#
# TopicNameStrategy: <тема>-value=ИмяЗаписи через запятую - тот же формат, что у -PschemaSubjects задач Gradle.
# Общая часть container-publish.sh, container-register.sh и скриптов хоста: список subject у проверки перед
# публикацией в Nexus и у регистрации один и тот же.
#
# schema_subjects <тема events> <тема devices> <тема failed-events>
schema_subjects() {
  # events - DeviceEvent, devices - DeviceInfo, failed-events - FailedEvent (исходные байты лежат внутри записи)
  printf '%s-value=DeviceEvent,%s-value=DeviceInfo,%s-value=FailedEvent' "$1" "$2" "$3"
}
