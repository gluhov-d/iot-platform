package com.github.gluhov.iotplatform.eventscollector.domain.port.out;

import java.util.OptionalLong;

public interface PublishedDeviceStore {

    boolean isPublished(String deviceId);

    void markPublished(String deviceId);

    /** Время исходного события последнего опубликованного снимка устройства (мс Unix) или пусто. */
    OptionalLong lastSnapshotAt(String deviceId);

    /** Запомнить время опубликованного снимка: более старый снимок того же устройства не публикуется. */
    void markSnapshotAt(String deviceId, long eventTimeMs);
}