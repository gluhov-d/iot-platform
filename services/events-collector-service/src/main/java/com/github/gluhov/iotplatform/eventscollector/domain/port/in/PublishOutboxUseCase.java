package com.github.gluhov.iotplatform.eventscollector.domain.port.in;

import com.github.gluhov.iotplatform.eventscollector.domain.model.BatchResult;

public interface PublishOutboxUseCase {

    BatchResult publishPendingBatch();
}
