package com.github.gluhov.iotplatform.eventscollector.service.adapter.in.scheduler;

import com.github.gluhov.iotplatform.eventscollector.adapter.in.scheduler.OutboxPublisherJob;
import com.github.gluhov.iotplatform.eventscollector.domain.port.in.PublishOutboxUseCase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherJobTest {

    @Mock
    private PublishOutboxUseCase publishOutbox;
    @InjectMocks
    private OutboxPublisherJob job;

    @Test
    @DisplayName("Run job then one publish iteration")
    void givenJob_whenRun_thenOneIteration() {
        job.run();

        verify(publishOutbox).publishPendingBatch();
    }

    @Test
    @DisplayName("Iteration fails then scheduler keeps running")
    void givenIterationFailure_whenRun_thenNoException() {
        when(publishOutbox.publishPendingBatch()).thenThrow(new IllegalStateException("clickhouse down"));

        assertThatCode(job::run).doesNotThrowAnyException();
    }
}
