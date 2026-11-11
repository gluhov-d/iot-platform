package com.github.gluhov.iotplatform.devicecollector.domain.model;

import java.time.Instant;

public record SensorState(String sensorId, String sensorType, String unit, String value, Instant measuredAt, String status) {}