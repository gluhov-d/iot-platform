package com.github.gluhov.iotplatform.failedeventsprocessor.adapter.out.s3;

import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveDeniedException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveUnavailableException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.PutOutcome;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.port.out.FailedEventArchive;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.Map;

/**
 * Условная запись объекта в MinIO: {@code If-None-Match: *} (R5). Повтор того же ключа отвечает 412 - это DUPLICATE,
 * а не ошибка. Запись обёрнута в Observation {@code s3.put-object} с тегом bucket (R8).
 */
public class S3FailedEventArchive implements FailedEventArchive {

    static final String OBSERVATION_NAME = "s3.put-object";
    private static final int STATUS_PRECONDITION_FAILED = 412;
    private static final int STATUS_FORBIDDEN = 403;

    private final S3Client client;
    private final String bucket;
    private final ObservationRegistry registry;

    public S3FailedEventArchive(S3Client client, String bucket, ObservationRegistry registry) {
        this.client = client;
        this.bucket = bucket;
        this.registry = registry;
    }

    @Override
    public PutOutcome put(String key, byte[] body, Map<String, String> metadata) {
        Observation observation = Observation.createNotStarted(OBSERVATION_NAME, registry)
                .lowCardinalityKeyValue("bucket", bucket);
        return observation.observe(() -> doPut(key, body, metadata));
    }

    private PutOutcome doPut(String key, byte[] body, Map<String, String> metadata) {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("application/json")
                .metadata(metadata)
                .ifNoneMatch("*")
                .build();
        try {
            client.putObject(request, RequestBody.fromBytes(body));
            return PutOutcome.CREATED;
        } catch (S3Exception e) {
            if (e.statusCode() == STATUS_PRECONDITION_FAILED) {
                return PutOutcome.DUPLICATE;
            }
            if (e.statusCode() == STATUS_FORBIDDEN) {
                throw new ArchiveDeniedException("Архив отказал в записи " + key, e);
            }
            throw new ArchiveUnavailableException("Архив вернул ошибку " + e.statusCode() + " для " + key, e);
        } catch (SdkClientException e) {
            throw new ArchiveUnavailableException("Архив недоступен или не ответил в срок: " + key, e);
        }
    }
}
