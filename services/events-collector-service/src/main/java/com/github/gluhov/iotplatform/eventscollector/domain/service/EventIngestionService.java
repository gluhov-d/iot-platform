package com.github.gluhov.iotplatform.eventscollector.domain.service;

import com.github.gluhov.iotplatform.events.avro.DeviceEvent;
import com.github.gluhov.iotplatform.eventscollector.domain.exception.EventPersistenceException;
import com.github.gluhov.iotplatform.eventscollector.domain.model.DeviceEventRecord;
import com.github.gluhov.iotplatform.eventscollector.domain.model.OutboxRecord;
import com.github.gluhov.iotplatform.eventscollector.domain.port.in.IngestEventUseCase;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.CollectorMetrics;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.DeviceEventRepository;
import com.github.gluhov.iotplatform.eventscollector.domain.port.out.OutboxRepository;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import lombok.RequiredArgsConstructor;

import java.time.Clock;
import java.time.Instant;

@RequiredArgsConstructor
public class EventIngestionService implements IngestEventUseCase {

    private final DeviceEventValidator validator;
    private final DeviceEventMapper mapper;
    private final DeviceEventRepository eventRepository;
    private final OutboxRepository outboxRepository;
    private final CollectorMetrics metrics;
    private final ObservationRegistry observationRegistry;
    private final Clock clock;

    @Override
    public void ingest(DeviceEvent event) {
        step("validate").observe(() -> validator.validate(event));

        Instant now = clock.instant();
        DeviceEventRecord eventRecord = mapper.toEventRecord(event, now);
        OutboxRecord outboxRecord = mapper.toInitialOutbox(event, now);

        persist("insert device_events", () -> eventRepository.insert(eventRecord));
        persist("insert device_outbox", () -> outboxRepository.insert(outboxRecord));

        metrics.outboxCandidateCreated();
        metrics.eventProcessed();
    }

    private void persist(String name, Runnable action) {
        try {
            step(name).observe(action);
        } catch (RuntimeException e) {
            metrics.eventPersistFailed();
            throw new EventPersistenceException(name + " не выполнен", e);
        }
    }

    private Observation step(String name) {
        return Observation.createNotStarted("events.ingest.step", observationRegistry)
                .contextualName(name)
                .lowCardinalityKeyValue("step", name);
    }
}
