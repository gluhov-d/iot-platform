package com.github.gluhov.iotplatform.eventscollector.domain.port.out;

import com.github.gluhov.iotplatform.eventscollector.domain.model.PublicationMarkResult;

import java.util.OptionalLong;
import java.util.Optional;

public interface PublishedDeviceStore {

    boolean isPublished(String deviceId);

    /** Время исходного события последнего опубликованного снимка устройства (мс Unix) или пусто. */
    OptionalLong lastSnapshotAt(String deviceId);

    /** Отпечаток данных устройства из последнего опубликованного снимка или пусто. */
    Optional<String> fingerprint(String deviceId);

    /**
     * Одной атомарной операцией: проверить, что аренда принадлежит {@code ownerToken}, и записать отметки публикации -
     * устройство опубликовано ({@code published}), время снимка ({@code snapshotAt}) и отпечаток ({@code fingerprint},
     * null - не менять; пишется только вместе с временем снимка).
     * Время снимка и отпечаток не откатываются: если в хранилище снимок новее {@code snapshotAt}, они не меняются.
     * Аренда не наша - не записывается ничего. Для публикации вызывается только после подтверждения Kafka.
     */
    PublicationMarkResult markPublication(String ownerToken, String deviceId, boolean published,
                                          OptionalLong snapshotAt, String fingerprint);
}
