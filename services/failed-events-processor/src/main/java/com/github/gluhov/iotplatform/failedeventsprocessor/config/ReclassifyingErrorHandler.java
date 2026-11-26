package com.github.gluhov.iotplatform.failedeventsprocessor.config;

import com.github.gluhov.iotplatform.failedeventsprocessor.adapter.in.kafka.KafkaErrorClassifier;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BackOffHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.util.backoff.BackOff;

import java.util.List;

/**
 * Обработчик ошибок, который перечитывает классификацию на каждом сбое (R28). DefaultErrorHandler выбирает BackOff
 * один раз на запись, при первом сбое, и меняет его только при смене класса исключения. Таймаут Schema Registry и
 * ответ 404 приходят одинаково (DeserializationException), поэтому запись, начавшаяся с временного сбоя, осталась бы
 * в повторах и после 404.
 * <p>
 * Нечитаемая запись без временной причины сбрасывает состояние повторов потока: при следующем разборе BackOff
 * выбирается заново по {@code KafkaConfig.backOffFor} (без повторов) и запись сразу уходит в восстановитель.
 * Остальные сбои обрабатываются как у DefaultErrorHandler: без изменений.
 */
class ReclassifyingErrorHandler extends DefaultErrorHandler {

    ReclassifyingErrorHandler(ConsumerRecordRecoverer recoverer, BackOff backOff, BackOffHandler backOffHandler) {
        super(recoverer, backOff, backOffHandler);
    }

    @Override
    public boolean handleOne(Exception thrownException, ConsumerRecord<?, ?> record, Consumer<?, ?> consumer,
                             MessageListenerContainer container) {
        reclassify(thrownException);
        return super.handleOne(thrownException, record, consumer, container);
    }

    @Override
    public void handleRemaining(Exception thrownException, List<ConsumerRecord<?, ?>> records,
                                Consumer<?, ?> consumer, MessageListenerContainer container) {
        reclassify(thrownException);
        super.handleRemaining(thrownException, records, consumer, container);
    }

    private void reclassify(Exception exception) {
        if (KafkaErrorClassifier.isUnreadable(exception) && !KafkaErrorClassifier.isTransient(exception)) {
            clearThreadState();
        }
    }
}
