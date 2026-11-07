package com.github.gluhov.iotplatform.devicecollector.adapter.in.kafka;

import com.github.gluhov.iotplatform.devicecollector.domain.exception.DeviceNotStoredException;
import com.github.gluhov.iotplatform.devicecollector.domain.exception.InvalidDeviceInfoException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.MessagePosition;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessingResult;
import com.github.gluhov.iotplatform.devicecollector.domain.port.in.ProcessDeviceInfoUseCase;
import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;

/**
 * Приём DeviceInfo из devices: по одному сообщению. Offset подтверждается только после COMMIT в PostgreSQL;
 * ошибку разбирает обработчик ошибок контейнера (KafkaConfig): повторы или devices-dlt.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class DeviceInfoListener {
    private final ProcessDeviceInfoUseCase processDeviceInfo;

    @KafkaListener(
            id = "devices-listener",
            topics = "${app.kafka.devices-topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            batch = "false"
    )
    public void onDeviceInfo(ConsumerRecord<String, DeviceInfo> record, Acknowledgment ack) {
        DeviceInfo info = record.value();
        try (var topic = MDC.putCloseable("topic", record.topic());
             var partition = MDC.putCloseable("partition", String.valueOf(record.partition()));
             var offset = MDC.putCloseable("offset", String.valueOf(record.offset()));
             var deviceId = MDC.putCloseable("deviceId", info == null ? record.key() : info.getDeviceId());
             var eventId = MDC.putCloseable("eventId", info == null ? null : info.getEventId());
             var eventType = MDC.putCloseable("eventType", info == null ? null : info.getEventType().name())) {
            ProcessingResult result;
            try {
                result = processDeviceInfo.process(record.key(), info,
                        new MessagePosition(record.topic(), record.partition(), record.offset(), deliveryAttempt(record)));
            } catch (RuntimeException e) {
                logFailure(e);
                throw e;
            }
            // Транзакция на шарде завершена (COMMIT) - только теперь offset
            ack.acknowledge();
            try (var shard = MDC.putCloseable("shard", result.shard());
                 var status = MDC.putCloseable("status", result.outcome().name())) {
                log.info("Device info {}, offset acknowledged", result.outcome().label());
            }
        }
    }

    // Заголовок ставит контейнер (KafkaConfig.deliveryAttemptHeader); нет заголовка - первая доставка
    static int deliveryAttempt(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(KafkaHeaders.DELIVERY_ATTEMPT);
        return header == null || header.value().length != Integer.BYTES ? 1 : ByteBuffer.wrap(header.value()).getInt();
    }

    private static void logFailure(RuntimeException e) {
        String shard = e instanceof DeviceNotStoredException notStored ? notStored.shard() : ProcessingResult.UNKNOWN_SHARD;
        String status = e instanceof InvalidDeviceInfoException ? "REJECTED" : "FAILED";
        try (var s = MDC.putCloseable("shard", shard);
             var st = MDC.putCloseable("status", status);
             var c = MDC.putCloseable("errorClass", e.getClass().getName());
             var m = MDC.putCloseable("errorMessage", String.valueOf(e.getMessage()))) {
            log.warn("Device info not processed, offset not acknowledged");
        }
    }
}