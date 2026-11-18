package com.github.gluhov.iotplatform.devicecollector.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.ApplicationListener;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.event.ConsumerStartingEvent;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Смещения группы в теме devices при auto-offset-reset = none (ADR-0011):
 * <ul>
 *   <li>раздел без подтверждённого смещения (новая группа, новый раздел) читается с начала темы: перед первым poll
 *       обработчика (ConsumerStartingEvent) группе через Admin выставляется самое раннее смещение таких разделов.
 *       Через rebalance listener нельзя: Spring Kafka для @KafkaListener спрашивает position() раньше него, и с none
 *       это NoOffsetForPartitionException;</li>
 *   <li>подтверждённое смещение вне диапазона темы (обработчик отстал дольше retention.ms или группе вернули смещение
 *       копии старше срока хранения): часть сообщений удалена до обработки, применять более поздние нельзя -
 *       удалённое администратором устройство вернулось бы событием роутера. KafkaConfig останавливает обработчик,
 *       здесь хранится, какие разделы и с какими смещениями вышли за диапазон: health DOWN до следующего запуска.</li>
 * </ul>
 */
@Slf4j
@Component
public class DevicesConsumerOffsets implements ApplicationListener<ConsumerStartingEvent>, HealthIndicator {
    private final Supplier<Admin> admins;
    private final String topic;
    private final String group;
    private final AtomicReference<Map<TopicPartition, Long>> outOfRange = new AtomicReference<>(Map.of());

    @Autowired
    public DevicesConsumerOffsets(KafkaAdmin kafkaAdmin, KafkaProperties kafkaProperties,
                                  @Value("${spring.kafka.consumer.group-id}") String group) {
        this(() -> Admin.create(kafkaAdmin.getConfigurationProperties()), kafkaProperties.devicesTopic(), group);
    }

    DevicesConsumerOffsets(Supplier<Admin> admins, String topic, String group) {
        this.admins = admins;
        this.topic = topic;
        this.group = group;
    }

    /** Поток обработчика до первого poll: группа ещё не вступила, смещения можно менять. */
    @Override
    public void onApplicationEvent(ConsumerStartingEvent event) {
        // Новый запуск обработчика (после решения оператора или перезапуска сервиса)
        outOfRange.set(Map.of());
        try {
            initializeMissingOffsets();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | RuntimeException e) {
            // Смещения не выставлены: раздел без смещения остановит обработчик (NoOffsetForPartitionException) -
            // тихого чтения не с того места не будет
            log.warn("Initial offsets of group {} for {} are not set: {}", group, topic, e.toString());
        }
    }

    /** Разделы темы без смещения группы получают самое раннее смещение (чтение с начала). */
    void initializeMissingOffsets() throws ExecutionException, InterruptedException {
        try (Admin admin = admins.get()) {
            List<TopicPartition> partitions = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic)
                    .partitions().stream()
                    .map(info -> new TopicPartition(topic, info.partition()))
                    .toList();
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(group)
                    .partitionsToOffsetAndMetadata().get();
            Map<TopicPartition, OffsetSpec> missing = new LinkedHashMap<>();
            partitions.stream()
                    .filter(partition -> committed.get(partition) == null)
                    .forEach(partition -> missing.put(partition, OffsetSpec.earliest()));
            if (missing.isEmpty()) {
                return;
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliest = admin.listOffsets(missing).all().get();
            Map<TopicPartition, OffsetAndMetadata> start = new LinkedHashMap<>();
            earliest.forEach((partition, info) -> start.put(partition, new OffsetAndMetadata(info.offset())));
            admin.alterConsumerGroupOffsets(group, start).all().get();
            log.info("No committed offset of group {} for {}: reading from the beginning of the topic", group,
                    missing.keySet());
        }
    }

    /** Смещения группы вне диапазона темы: раздел -> смещение, которое уже удалено из темы. */
    void offsetOutOfRange(Map<TopicPartition, Long> partitions) {
        Map<TopicPartition, Long> sorted = new TreeMap<>((a, b) -> Integer.compare(a.partition(), b.partition()));
        sorted.putAll(partitions);
        if (outOfRange.getAndSet(Collections.unmodifiableMap(sorted)).isEmpty()) {
            log.error("Committed offset is out of range for {}: messages were removed by retention before processing."
                    + " The devices listener is stopped: restore the shards from a backup or move the group offset"
                    + " explicitly (ADR-0011)", sorted);
        }
    }

    @Override
    public Health health() {
        Map<TopicPartition, Long> partitions = outOfRange.get();
        if (partitions.isEmpty()) {
            return Health.up().build();
        }
        return Health.down()
                .withDetail("reason", "committed offset out of range: messages removed by retention before processing")
                .withDetail("partitions", partitions.toString())
                .build();
    }
}
