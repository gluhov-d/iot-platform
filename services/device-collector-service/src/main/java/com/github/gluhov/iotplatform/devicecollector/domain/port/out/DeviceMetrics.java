package com.github.gluhov.iotplatform.devicecollector.domain.port.out;

import com.github.gluhov.iotplatform.devicecollector.domain.model.FailureReason;
import com.github.gluhov.iotplatform.devicecollector.domain.model.ProcessingOutcome;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StorageFailureReason;

import java.time.Duration;

/**
 * Прикладные метрики - в сообщениях, как в задании: сообщение получено один раз (повторы после ошибки не считаются)
 * и завершается ровно одним исходом - processed, duplicate или failed (отложено в devices-dlt).
 * Поэтому received = processed + duplicate + failed, когда в обработке нет сообщений. Попытки, завершившиеся
 * ошибкой, - отдельный счётчик devices_processing_errors_total.
 */
public interface DeviceMetrics {

    /** devices_messages_received_total. */
    void received();

    /** APPLIED, STALE - devices_messages_processed_total{outcome}; DUPLICATE - devices_messages_duplicate_total. */
    void processed(ProcessingOutcome outcome);

    /** devices_messages_failed_total{reason}: сообщение завершилось ошибкой - отложено в devices-dlt. */
    void failed(FailureReason reason);

    /** devices_processing_errors_total{reason}: попытка обработки завершилась ошибкой (повтор или devices-dlt). */
    void processingError(FailureReason reason);

    /** devices_dlt_published_total{reason}: сообщение записано в devices-dlt. */
    void dltPublished(FailureReason reason);

    /** devices_processing_seconds{outcome}: полное время обработки сообщения. */
    void processingTime(String outcome, Duration duration);

    /** devices_db_write_seconds{shard, result}: время транзакции в PostgreSQL через Proxy. */
    void dbWrite(String shard, boolean success, Duration duration);

    /** devices_shard_route_fail_total{shard, reason}: ошибка маршрутизации или доступа к шарду. */
    void shardRouteFailed(String shard, StorageFailureReason reason);
}