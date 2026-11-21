package com.github.gluhov.iotplatform.devicecollector.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class NoOffsetRecoveringConsumerTest {
    private static final TopicPartition P3 = new TopicPartition("devices", 3);

    private final Consumer<String, String> delegate = mock(Consumer.class);
    private final Consumer<String, String> consumer = NoOffsetRecoveringConsumer.wrap(delegate);

    @Test
    @DisplayName("position() without a committed offset: those partitions go to the beginning, the call is repeated once")
    void givenNoOffsetInPosition_whenPosition_thenSeekToBeginningAndPositionReturned() {
        when(delegate.position(P3)).thenThrow(new NoOffsetForPartitionException(Set.of(P3))).thenReturn(0L);

        assertThat(consumer.position(P3)).isZero();

        verify(delegate).seekToBeginning(Set.of(P3));
        verify(delegate, times(2)).position(P3);
    }

    @Test
    @DisplayName("poll() without a committed offset: seek to the beginning and one repeat of poll")
    void givenNoOffsetInPoll_whenPoll_thenSeekToBeginningAndPollRepeated() {
        ConsumerRecords<String, String> records = ConsumerRecords.empty();
        when(delegate.poll(any(Duration.class))).thenThrow(new NoOffsetForPartitionException(Set.of(P3))).thenReturn(records);

        assertThat(consumer.poll(Duration.ofMillis(10))).isSameAs(records);

        verify(delegate).seekToBeginning(Set.of(P3));
        verify(delegate, times(2)).poll(any(Duration.class));
    }

    @Test
    @DisplayName("The repeat is one: a second NoOffsetForPartitionException is thrown to the caller")
    void givenNoOffsetTwice_whenPosition_thenThrown() {
        NoOffsetForPartitionException second = new NoOffsetForPartitionException(Set.of(P3));
        when(delegate.position(P3)).thenThrow(new NoOffsetForPartitionException(Set.of(P3))).thenThrow(second);

        assertThatThrownBy(() -> consumer.position(P3)).isSameAs(second);

        verify(delegate, times(1)).seekToBeginning(Set.of(P3));
    }

    @Test
    @DisplayName("Offset out of range is not intercepted: thrown as is, no seek (the listener stops, ADR-0011)")
    void givenOffsetOutOfRange_whenPositionOrPoll_thenThrownWithoutSeek() {
        OffsetOutOfRangeException outOfRange = new OffsetOutOfRangeException("out of range", Map.of(P3, 5L));
        when(delegate.position(P3)).thenThrow(outOfRange);
        when(delegate.poll(any(Duration.class))).thenThrow(outOfRange);

        assertThatThrownBy(() -> consumer.position(P3)).isSameAs(outOfRange);
        assertThatThrownBy(() -> consumer.poll(Duration.ofMillis(10))).isSameAs(outOfRange);

        verify(delegate, never()).seekToBeginning(any());
    }

    @Test
    @DisplayName("Other methods are delegated as they are, their exceptions are not wrapped")
    void givenOtherMethods_whenCalled_thenDelegated() {
        when(delegate.assignment()).thenReturn(Set.of(P3));
        when(delegate.committed(Set.of(P3))).thenThrow(new IllegalStateException("boom"));

        assertThat(consumer.assignment()).containsExactly(P3);
        assertThatThrownBy(() -> consumer.committed(Set.of(P3))).isInstanceOf(IllegalStateException.class).hasMessage("boom");
        consumer.seekToBeginning(Set.of(P3));

        verify(delegate).seekToBeginning(Set.of(P3));
    }
}
