package com.github.gluhov.iotplatform.devicecollector.domain.model;

/**
 * Команда администратора (DeviceInfo.operation) - то же, что операция в устройстве. У событий роутера команды нет.
 */
public enum CommandType {
    CREATE, UPDATE, DELETE
}
