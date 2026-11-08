package com.github.gluhov.iotplatform.eventscollector.domain.model;

import java.time.Instant;
import java.time.LocalDate;

/**
 * Строка device_events.
 *
 * @param deviceSnapshot снимок устройства в формате Avro JSON; пустая строка - событие без снимка
 * @param sourceSystem   система-источник; пустая строка - не указана
 * @param payload        сырая нагрузка исходного события или null
 * @param deviceName     имя устройства из снимка; пустая строка - снимка нет или поле не задано
 * @param osName         операционная система устройства; пустая строка - не задана
 * @param osVersion      версия операционной системы; пустая строка - не задана
 * @param firmwareVersion версия прошивки; пустая строка - не задана
 */
public record DeviceEventRecord(
        String deviceId,
        String eventId,
        LocalDate eventDate,
        long timestampMs,
        String eventType,
        Instant ingestedAt,
        String deviceSnapshot,
        String sourceSystem,
        String payload,
        String deviceName,
        String osName,
        String osVersion,
        String firmwareVersion) {}