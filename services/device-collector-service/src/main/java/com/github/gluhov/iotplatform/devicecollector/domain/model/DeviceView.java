package com.github.gluhov.iotplatform.devicecollector.domain.model;

import java.time.Instant;

/** Устройство из реестра для чтения по ключу. */
public record DeviceView(DeviceState device,
                         long seenCount,
                         Instant firstSeenAt,
                         Instant lastEventAt,
                         String lastEventId,
                         String lastEventType,
                         Instant deletedAt,
                         String shard) {}