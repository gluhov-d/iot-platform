package com.github.gluhov.iotplatform.eventscollector.service.adapter.out.kafka;

import com.github.gluhov.iotplatform.events.avro.DeviceInfo;
import com.github.gluhov.iotplatform.eventscollector.adapter.out.kafka.DeviceInfoPublishException;
import com.github.gluhov.iotplatform.eventscollector.adapter.out.kafka.KafkaDeviceInfoProducer;
import com.github.gluhov.iotplatform.eventscollector.config.KafkaProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static com.github.gluhov.iotplatform.eventscollector.data.TestData.DEVICE_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaDeviceInfoProducerTest {
    private static final String DEVICES_TOPIC = "devices";

    @Mock
    private KafkaTemplate<String, DeviceInfo> kafkaTemplate;

    private KafkaDeviceInfoProducer producer;

    @BeforeEach
    void setUp() {
        KafkaProperties properties = new KafkaProperties("events", DEVICES_TOPIC, Duration.ofMillis(200));
        producer = new KafkaDeviceInfoProducer(kafkaTemplate, properties);
    }

    @Test
    @DisplayName("Publish device then DeviceInfo sent to devices keyed by deviceId")
    void givenDevice_whenPublish_thenSendKeyedByDeviceId() {
        when(kafkaTemplate.send(eq(DEVICES_TOPIC), eq(DEVICE_ID), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        producer.publish(DEVICE_ID, 123L);

        ArgumentCaptor<DeviceInfo> infoArgumentCaptor = ArgumentCaptor.forClass(DeviceInfo.class);
        verify(kafkaTemplate).send(eq(DEVICES_TOPIC), eq(DEVICE_ID), infoArgumentCaptor.capture());
        assertThat(infoArgumentCaptor.getValue().getDeviceId()).isEqualTo(DEVICE_ID);
        assertThat(infoArgumentCaptor.getValue().getEmittedAtMs()).isEqualTo(123L);
    }

    @Test
    @DisplayName("Broker rejects message then publish exception")
    void givenBrokerError_whenPublish_thenThrowException() {
        when(kafkaTemplate.send(any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new TimeoutException("no leader")));

        assertThatThrownBy(() -> producer.publish(DEVICE_ID, 1L))
                .isInstanceOf(DeviceInfoPublishException.class)
                .hasRootCauseMessage("no leader");
    }

    @Test
    @DisplayName("No acknowledgement within timeout then publish exception")
    void givenNoAcknowledgement_whenPublish_thenThrowTimeout() {
        when(kafkaTemplate.send(any(), any(), any())).thenReturn(new CompletableFuture<>());

        assertThatThrownBy(() -> producer.publish(DEVICE_ID, 1L))
                .isInstanceOf(DeviceInfoPublishException.class)
                .hasMessageContaining("No Kafka acknowledgement");
    }

    @Test
    @DisplayName("Serialization fails synchronously then publish exception")
    void givenSerializationError_whenPublish_thenThrowException() {
        when(kafkaTemplate.send(any(), any(), any())).thenThrow(new IllegalStateException("schema registry down"));

        assertThatThrownBy(() -> producer.publish(DEVICE_ID, 1L))
                .isInstanceOf(DeviceInfoPublishException.class);
    }
}
