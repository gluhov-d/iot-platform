package com.github.gluhov.iotplatform.devicecollector.config;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.health.contributor.Status;
import org.springframework.kafka.event.ConsumerStartingEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DevicesConsumerOffsetsTest {
    private static final String TOPIC = "devices";
    private static final String GROUP = "device-collector";
    private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition P1 = new TopicPartition(TOPIC, 1);
    private static final TopicPartition P2 = new TopicPartition(TOPIC, 2);

    private final Admin admin = mock(Admin.class);
    private final DevicesConsumerOffsets offsets = new DevicesConsumerOffsets(() -> admin, TOPIC, GROUP);

    @Test
    @DisplayName("Partitions without a committed offset get the earliest offset, the others keep theirs")
    void givenPartitionsWithoutOffset_whenListenerStarts_thenOnlyTheyStartFromEarliest() {
        topicWithPartitions(3);
        Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
        committed.put(P0, new OffsetAndMetadata(42L));
        committed.put(P1, null);
        committedOffsets(committed);
        ListOffsetsResult earliest = mock(ListOffsetsResult.class);
        when(earliest.all()).thenReturn(KafkaFuture.completedFuture(Map.of(
                P1, new ListOffsetsResult.ListOffsetsResultInfo(5L, -1L, Optional.empty()),
                P2, new ListOffsetsResult.ListOffsetsResultInfo(0L, -1L, Optional.empty()))));
        // OffsetSpec без equals: запрошенные разделы проверяются по ключам ниже
        when(admin.listOffsets(anyMap())).thenReturn(earliest);
        AlterConsumerGroupOffsetsResult altered = mock(AlterConsumerGroupOffsetsResult.class);
        when(altered.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.alterConsumerGroupOffsets(eq(GROUP), anyMap())).thenReturn(altered);

        offsets.onApplicationEvent(new ConsumerStartingEvent(this, this));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<TopicPartition, OffsetSpec>> requested = ArgumentCaptor.forClass(Map.class);
        verify(admin).listOffsets(requested.capture());
        assertThat(requested.getValue()).containsOnlyKeys(P1, P2);
        verify(admin).alterConsumerGroupOffsets(GROUP, Map.of(P1, new OffsetAndMetadata(5L), P2, new OffsetAndMetadata(0L)));
        verify(admin).close();
    }

    @Test
    @DisplayName("All partitions have a committed offset: the group offsets are not changed")
    void givenCommittedOffsets_whenListenerStarts_thenNothingChanged() {
        topicWithPartitions(1);
        committedOffsets(Map.of(P0, new OffsetAndMetadata(7L)));

        offsets.onApplicationEvent(new ConsumerStartingEvent(this, this));

        verify(admin, never()).listOffsets(anyMap());
        verify(admin, never()).alterConsumerGroupOffsets(any(), anyMap());
    }

    @Test
    @DisplayName("Kafka unavailable at start: logged, the listener start is not blocked by an exception")
    void givenAdminFailure_whenListenerStarts_thenNoException() {
        DescribeTopicsResult described = mock(DescribeTopicsResult.class);
        KafkaFuture<Map<String, TopicDescription>> failed = KafkaFuture.completedFuture(null)
                .thenApply(ignored -> { throw new IllegalStateException("broker down"); });
        when(described.allTopicNames()).thenReturn(failed);
        when(admin.describeTopics(List.of(TOPIC))).thenReturn(described);

        offsets.onApplicationEvent(new ConsumerStartingEvent(this, this));

        verify(admin, never()).alterConsumerGroupOffsets(any(), anyMap());
        assertThat(offsets.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    @DisplayName("Offset out of range: health DOWN with the partitions; the next listener start brings it back UP")
    void givenOffsetOutOfRange_whenHealth_thenDownUntilNextStart() {
        assertThat(offsets.health().getStatus()).isEqualTo(Status.UP);

        offsets.offsetOutOfRange(Map.of(P2, 5L, P0, 3L));

        assertThat(offsets.health().getStatus()).isEqualTo(Status.DOWN);
        assertThat(offsets.health().getDetails()).containsEntry("partitions", "{devices-0=3, devices-2=5}");

        topicWithPartitions(1);
        committedOffsets(Map.of(P0, new OffsetAndMetadata(7L)));
        offsets.onApplicationEvent(new ConsumerStartingEvent(this, this));

        assertThat(offsets.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    @DisplayName("Listener stopped for any reason: health DOWN with the reason; running again: UP")
    void givenListenerStopped_whenHealth_thenDownWithReason() {
        AtomicBoolean running = new AtomicBoolean(true);
        DevicesConsumerOffsets watched = new DevicesConsumerOffsets(() -> admin, TOPIC, GROUP, running::get);
        assertThat(watched.health().getStatus()).isEqualTo(Status.UP);

        running.set(false);

        assertThat(watched.health().getStatus()).isEqualTo(Status.DOWN);
        assertThat(watched.health().getDetails()).containsEntry("reason", "devices listener is not running");

        running.set(true);

        assertThat(watched.health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    @DisplayName("Offset out of range and listener stopped: the reason names the offsets, not just the stop")
    void givenOffsetOutOfRangeAndStopped_whenHealth_thenOutOfRangeReason() {
        DevicesConsumerOffsets watched = new DevicesConsumerOffsets(() -> admin, TOPIC, GROUP, () -> false);

        watched.offsetOutOfRange(Map.of(P0, 3L));

        assertThat(watched.health().getDetails()).containsEntry("partitions", "{devices-0=3}")
                .containsEntry("reason", "committed offset out of range: messages removed by retention before processing");
    }

    private void topicWithPartitions(int count) {
        Node node = new Node(1, "kafka", 9092);
        List<TopicPartitionInfo> partitions = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            partitions.add(new TopicPartitionInfo(i, node, List.of(node), List.of(node)));
        }
        DescribeTopicsResult described = mock(DescribeTopicsResult.class);
        when(described.allTopicNames()).thenReturn(KafkaFuture.completedFuture(
                Map.of(TOPIC, new TopicDescription(TOPIC, false, partitions))));
        when(admin.describeTopics(List.of(TOPIC))).thenReturn(described);
    }

    private void committedOffsets(Map<TopicPartition, OffsetAndMetadata> committed) {
        ListConsumerGroupOffsetsResult result = mock(ListConsumerGroupOffsetsResult.class);
        when(result.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(committed));
        when(admin.listConsumerGroupOffsets(GROUP)).thenReturn(result);
    }
}
