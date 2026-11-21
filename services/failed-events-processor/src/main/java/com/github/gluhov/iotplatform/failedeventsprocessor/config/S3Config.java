package com.github.gluhov.iotplatform.failedeventsprocessor.config;

import com.github.gluhov.iotplatform.failedeventsprocessor.adapter.out.s3.S3FailedEventArchive;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.port.out.FailedEventArchive;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.service.ArchiveService;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.net.URI;
import java.time.Clock;

@Configuration
@EnableConfigurationProperties(FailedEventsS3Properties.class)
public class S3Config {

    @Bean(destroyMethod = "close")
    S3Client s3Client(FailedEventsS3Properties properties) {
        FailedEventsS3Properties.S3 s3 = properties.s3();
        return S3Client.builder()
                .endpointOverride(URI.create(s3.endpoint()))
                .region(Region.of(s3.region()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(s3.accessKey(), s3.secretKey())))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .httpClientBuilder(ApacheHttpClient.builder().connectionTimeout(s3.connectTimeout()))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(s3.apiCallTimeout())
                        .apiCallAttemptTimeout(s3.apiCallAttemptTimeout())
                        .build())
                .build();
    }

    @Bean
    FailedEventArchive failedEventArchive(S3Client s3Client, FailedEventsS3Properties properties,
                                          ObservationRegistry observationRegistry) {
        return new S3FailedEventArchive(s3Client, properties.bucket(), observationRegistry);
    }

    @Bean
    ArchiveService archiveService(FailedEventArchive failedEventArchive) {
        return new ArchiveService(failedEventArchive);
    }

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
