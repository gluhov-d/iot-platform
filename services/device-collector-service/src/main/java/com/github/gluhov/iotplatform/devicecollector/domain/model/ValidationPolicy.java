package com.github.gluhov.iotplatform.devicecollector.domain.model;

import java.time.Duration;

/** @param maxFutureSkew насколько время события может опережать часы сервиса */
public record ValidationPolicy(Duration maxFutureSkew) {}