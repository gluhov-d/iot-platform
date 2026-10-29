package com.github.gluhov.iotplatform.eventscollector.domain.port.out;

import com.github.gluhov.iotplatform.eventscollector.domain.model.OutboxRecord;

import java.util.List;

public interface OutboxRepository {

    void insert(OutboxRecord record);

    List<OutboxRecord> findPending(int maxAttempts, int limit);

    long countPending(int maxAttempts);
}
