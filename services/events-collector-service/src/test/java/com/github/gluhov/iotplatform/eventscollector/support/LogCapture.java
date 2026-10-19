package com.github.gluhov.iotplatform.eventscollector.support;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

// Перехват журнала одного класса в тесте: событие сохраняется вместе со снимком MDC на момент записи
public final class LogCapture implements AutoCloseable {
    private final Logger logger;
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>() {
        @Override
        protected void append(ILoggingEvent event) {
            // MDC события читается лениво: без снимка поля прочитались бы позже, когда область MDC уже закрыта
            event.prepareForDeferredProcessing();
            super.append(event);
        }
    };

    private LogCapture(Class<?> source) {
        logger = (Logger) LoggerFactory.getLogger(source);
        appender.start();
        logger.addAppender(appender);
    }

    public static LogCapture of(Class<?> source) {
        return new LogCapture(source);
    }

    // Единственное сообщение с таким уровнем и текстом
    public ILoggingEvent single(Level level, String message) {
        List<ILoggingEvent> found = List.copyOf(appender.list).stream()
                .filter(event -> event.getLevel() == level && message.equals(event.getFormattedMessage()))
                .toList();
        assertThat(found).as("%s log \"%s\"", level, message).hasSize(1);
        return found.getFirst();
    }

    @Override
    public void close() {
        logger.detachAppender(appender);
        appender.stop();
    }
}
