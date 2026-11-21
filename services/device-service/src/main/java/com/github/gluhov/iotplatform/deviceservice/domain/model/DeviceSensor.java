package com.github.gluhov.iotplatform.deviceservice.domain.model;

import java.time.Instant;

/** Сенсор устройства в модели чтения (элемент снимка devices.sensors). */
public record DeviceSensor(String sensorId,
                           String sensorType,
                           String unit,
                           String value,
                           Instant measuredAt,
                           String status) {
}
