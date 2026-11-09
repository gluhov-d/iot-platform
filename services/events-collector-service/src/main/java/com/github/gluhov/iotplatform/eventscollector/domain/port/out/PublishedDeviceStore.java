package com.github.gluhov.iotplatform.eventscollector.domain.port.out;

import java.util.OptionalLong;
import java.util.Optional;

public interface PublishedDeviceStore {

    boolean isPublished(String deviceId);

    void markPublished(String deviceId);

    /** Время исходного события последнего опубликованного снимка устройства (мс Unix) или пусто. */
    OptionalLong lastSnapshotAt(String deviceId);

    /** Запомнить время опубликованного снимка: более старый снимок того же устройства не публикуется. */
    void markSnapshotAt(String deviceId, long eventTimeMs);

    /** Отпечаток данных устройства из последнего опубликованного снимка или пусто. */
    Optional<String> fingerprint(String deviceId);

    /** Запомнить отпечаток опубликованного снимка; вызывается только после подтверждения Kafka. */
    void markFingerprint(String deviceId, String fingerprint);
}