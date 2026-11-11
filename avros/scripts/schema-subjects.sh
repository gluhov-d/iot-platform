# shellcheck shell=bash
# Subject'ы Schema Registry платформы (подключается через source, сама не запускается).
#
# TopicNameStrategy: <тема>-value=ИмяЗаписи через запятую - тот же формат, что у -PschemaSubjects задач Gradle.
# Общая часть container-publish.sh, container-register.sh и скриптов хоста: список subject у проверки перед
# публикацией в Nexus и у регистрации один и тот же.
#
# schema_subjects <тема events> <тема events-dlt> <тема devices> <тема devices-dlt>
schema_subjects() {
  # events и events-dlt - DeviceEvent; devices и devices-dlt - DeviceInfo.
  # В DLT-темы попадают и нечитаемые байты: они схемой не описываются
  printf '%s-value=DeviceEvent,%s-value=DeviceEvent,%s-value=DeviceInfo,%s-value=DeviceInfo' "$1" "$2" "$3" "$4"
}