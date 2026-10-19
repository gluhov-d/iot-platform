package com.github.gluhov.iotplatform.eventscollector.domain.port.out;

public interface PublishedDeviceStore {

    boolean isPublished(String deviceId);

    void markPublished(String deviceId);
}
