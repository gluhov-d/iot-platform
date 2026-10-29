package com.github.gluhov.iotplatform.eventscollector.domain.port.out;

import java.time.Duration;
import java.util.Optional;

public interface PublisherLease {

    Optional<String> tryAcquire(Duration ttl);

    // true - аренда все еще наша и продлена на ttl; false - истекла или ее уже взял другой экземпляр
    boolean extend(String ownerToken, Duration ttl);

    boolean release(String ownerToken);
}
