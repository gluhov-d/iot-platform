package com.github.gluhov.iotplatform.eventscollector.domain.port.out;

import java.time.Duration;
import java.util.Optional;

public interface PublisherLease {

    Optional<String> tryAcquire(Duration ttl);

    boolean release(String ownerToken);
}
