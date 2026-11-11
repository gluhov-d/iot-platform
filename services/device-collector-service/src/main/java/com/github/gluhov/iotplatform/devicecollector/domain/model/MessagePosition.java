package com.github.gluhov.iotplatform.devicecollector.domain.model;

/**
 * Откуда пришло сообщение: тема, раздел, offset - для журнала и логов.
 *
 * @param deliveryAttempt номер доставки обработчику (заголовок kafka_deliveryAttempt): 1 - первая, больше - повтор
 *                        после ошибки. Полученным сообщение считается один раз - при первой доставке
 */
public record MessagePosition(String topic, int partition, long offset, int deliveryAttempt) {

    public MessagePosition(String topic, int partition, long offset) {
        this(topic, partition, offset, 1);
    }

    public boolean firstDelivery() {
        return deliveryAttempt <= 1;
    }
}