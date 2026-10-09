package com.github.gluhov.iotplatform.eventscollector.config;

import com.github.gluhov.iotplatform.eventscollector.domain.exception.InvalidEventException;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration(proxyBeanMethods = false)
@Slf4j
public class KafkaConfig {
    private static final long INITIAL_BACKOFF_MS = 500L;
    private static final double BACKOFF_MULTIPLIER = 2.0;
    private static final long MAX_BACKOFF_MS = 10_000L;

    // Ошибки записи повторяются без ограничения попыток: входное событие нельзя терять.
    // Ошибки валидации и десериализации не повторяются, offset при этом не подтверждается.
    @Bean
    DefaultErrorHandler kafkaErrorHandler() {
        ExponentialBackOff backOff = new ExponentialBackOff(INITIAL_BACKOFF_MS, BACKOFF_MULTIPLIER);
        backOff.setMaxInterval(MAX_BACKOFF_MS);

        DefaultErrorHandler handler = new DefaultErrorHandler((record, exception) -> {
            Throwable root = rootCause(exception);
            try (var topic = MDC.putCloseable("topic", record.topic());
                var partition = MDC.putCloseable("partition", String.valueOf(record.partition()));
                var offset = MDC.putCloseable("offset", String.valueOf(record.offset()));
                var status = MDC.putCloseable("status", "REJECTED");
                var errorClass = MDC.putCloseable("errorClass", root.getClass().getName());
                var errorMessage = MDC.putCloseable("errorMessage", String.valueOf(root.getMessage()))) {
                log.error("Message rejected without retries: {}", root.toString());
            }
        }, backOff);
        handler.addNotRetryableExceptions(InvalidEventException.class);
        handler.setAckAfterHandle(false);
        return handler;
    }

    private static Throwable rootCause(Throwable e) {
        Throwable root = e;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root;
    }
}
