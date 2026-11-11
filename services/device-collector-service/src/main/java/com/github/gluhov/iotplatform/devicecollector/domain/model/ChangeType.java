package com.github.gluhov.iotplatform.devicecollector.domain.model;

/** Тип изменения устройства (DeviceInfo.eventType); SNAPSHOT - сообщение версии контрактов 1.0.0. */
public enum ChangeType {
    CREATED, UPDATED, DELETED, SNAPSHOT
}