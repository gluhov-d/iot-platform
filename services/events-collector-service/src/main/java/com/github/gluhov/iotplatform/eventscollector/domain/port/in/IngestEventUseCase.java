package com.github.gluhov.iotplatform.eventscollector.domain.port.in;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;

public interface IngestEventUseCase {

    void ingest(DeviceEvent event);
}
