package com.github.gluhov.iotplatform.failedeventsprocessor.adapter.out.s3;

import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveDeniedException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.exception.ArchiveUnavailableException;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.PutOutcome;
import io.micrometer.observation.tck.TestObservationRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3FailedEventArchiveTest {

    private static final String BUCKET = "failed-events";
    private static final String KEY = "events-collector-service/2026/11/21/VALIDATION_FAILED/events-2-1042.json";
    private static final byte[] BODY = "{}".getBytes();
    private static final Map<String, String> METADATA = Map.of("error-code", "VALIDATION_FAILED");

    @Test
    void givenNewKey_whenPut_thenPutObjectWithIfNoneMatchStarAndMetadata() {
        S3Client client = mock(S3Client.class);
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());
        S3FailedEventArchive archive = new S3FailedEventArchive(client, BUCKET, TestObservationRegistry.create());

        PutOutcome outcome = archive.put(KEY, BODY, METADATA);

        ArgumentCaptor<PutObjectRequest> request = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(client).putObject(request.capture(), any(RequestBody.class));
        assertThat(outcome).isEqualTo(PutOutcome.CREATED);
        assertThat(request.getValue().bucket()).isEqualTo(BUCKET);
        assertThat(request.getValue().key()).isEqualTo(KEY);
        assertThat(request.getValue().ifNoneMatch()).isEqualTo("*");
        assertThat(request.getValue().contentType()).isEqualTo("application/json");
        assertThat(request.getValue().metadata()).containsEntry("error-code", "VALIDATION_FAILED");
    }

    @Test
    void givenKeyAlreadyWritten_whenPut_then412IsDuplicate() {
        S3Client client = mock(S3Client.class);
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(S3Exception.builder().statusCode(412).message("At least one of the pre-conditions failed").build());
        S3FailedEventArchive archive = new S3FailedEventArchive(client, BUCKET, TestObservationRegistry.create());

        assertThat(archive.put(KEY, BODY, METADATA)).isEqualTo(PutOutcome.DUPLICATE);
    }

    @Test
    void givenTimeout_whenPut_thenUnavailable() {
        S3Client client = mock(S3Client.class);
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(SdkClientException.create("Unable to execute HTTP request: timed out"));
        S3FailedEventArchive archive = new S3FailedEventArchive(client, BUCKET, TestObservationRegistry.create());

        assertThatThrownBy(() -> archive.put(KEY, BODY, METADATA))
                .isInstanceOf(ArchiveUnavailableException.class);
    }

    @Test
    void givenServiceUnavailable_whenPut_thenUnavailable() {
        S3Client client = mock(S3Client.class);
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(S3Exception.builder().statusCode(503).message("Service Unavailable").build());
        S3FailedEventArchive archive = new S3FailedEventArchive(client, BUCKET, TestObservationRegistry.create());

        assertThatThrownBy(() -> archive.put(KEY, BODY, METADATA))
                .isInstanceOf(ArchiveUnavailableException.class);
    }

    @Test
    void givenAccessDenied_whenPut_thenDenied() {
        S3Client client = mock(S3Client.class);
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(S3Exception.builder().statusCode(403).message("Access Denied").build());
        S3FailedEventArchive archive = new S3FailedEventArchive(client, BUCKET, TestObservationRegistry.create());

        assertThatThrownBy(() -> archive.put(KEY, BODY, METADATA))
                .isInstanceOf(ArchiveDeniedException.class);
    }

    @Test
    void givenPut_whenObserved_thenSpanS3PutObjectWithBucketTag() {
        S3Client client = mock(S3Client.class);
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());
        TestObservationRegistry registry = TestObservationRegistry.create();
        S3FailedEventArchive archive = new S3FailedEventArchive(client, BUCKET, registry);

        archive.put(KEY, BODY, METADATA);

        io.micrometer.observation.tck.TestObservationRegistryAssert.assertThat(registry)
                .hasObservationWithNameEqualTo("s3.put-object")
                .that()
                .hasLowCardinalityKeyValue("bucket", BUCKET);
    }
}
