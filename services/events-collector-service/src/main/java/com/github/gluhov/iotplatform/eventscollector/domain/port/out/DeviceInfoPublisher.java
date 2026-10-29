package com.github.gluhov.iotplatform.eventscollector.domain.port.out;

public interface DeviceInfoPublisher {

    void publish(String deviceId, long emittedAtMs);
}
