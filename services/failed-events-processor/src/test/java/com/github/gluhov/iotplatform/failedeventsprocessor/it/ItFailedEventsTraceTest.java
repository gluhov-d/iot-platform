package com.github.gluhov.iotplatform.failedeventsprocessor.it;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Трасса записи в архив (FR-011; R8): traceId из заголовка traceparent попадает в объект, спан s3.put-object - потомок. */
class ItFailedEventsTraceTest extends AbstractItTest {

    @Test
    void givenTraceparent_whenArchived_thenObjectHasTraceIdAndS3SpanIsChildOfListenerSpan() throws Exception {
        long offset = nextSourceOffset();
        String traceparent = "00-" + TRACE_ID + "-" + PARENT_SPAN_ID + "-01";
        RecordMetadata sent = send(FAILED + "-" + offset,
                event(offset, TRACE_ID, Instant.parse("2026-11-21T10:15:31.480Z")),
                List.of(header("traceparent", traceparent.getBytes(StandardCharsets.UTF_8))));

        awaitCommitted(sent);

        String key = sourceKey(offset);
        awaitObject(key);
        JsonNode doc = readJson(key);
        assertThat(doc.get("traceId").asString()).isEqualTo(TRACE_ID);
        assertThat(head(key).metadata()).containsEntry("trace-id", TRACE_ID);

        SpanData s3Span = await().atMost(WAIT).until(() -> findSpan("s3.put-object", TRACE_ID), java.util.Optional::isPresent)
                .orElseThrow();
        assertThat(s3Span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("bucket")))
                .isEqualTo(BUCKET);
        SpanData listenerSpan = spans.getFinishedSpanItems().stream()
                .filter(span -> span.getTraceId().equals(TRACE_ID) && span.getSpanId().equals(s3Span.getParentSpanId()))
                .findFirst().orElseThrow(() -> new AssertionError("родитель s3.put-object не найден в трассе"));
        assertThat(listenerSpan.getKind()).isEqualTo(SpanKind.CONSUMER);
    }

    private java.util.Optional<SpanData> findSpan(String name, String traceId) {
        return spans.getFinishedSpanItems().stream()
                .filter(span -> span.getName().equals(name) && span.getTraceId().equals(traceId))
                .findFirst();
    }
}
