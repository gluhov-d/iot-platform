import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Возврат объекта ошибки в исходную тему (FR-024, ADR-0019). Запуск - replay-tool.sh: исходный запуск Java из одного файла в
 * одноразовом JDK-контейнере, клиенты Kafka и Jackson - из /opt/kafka/libs контейнера kafka.
 *
 *   produce BOOTSTRAP КЛЮЧ_ОБЪЕКТА   - JSON объекта со стандартного ввода -> одно сообщение в sourceTopic:
 *                                      байты key и value и заголовки из base64 как есть, без пересериализации,
 *                                      к ним добавлен заголовок x-replayed-from = ключ объекта;
 *                                      код 3, если запись нечитаема (kind = unreadable): исходного сообщения в объекте нет
 *   dump BOOTSTRAP ТЕМА              - все сообщения темы по строке: раздел, key, value и заголовки в шестнадцатеричном
 *                                      виде (проверка тестом replay-test.sh)
 *
 * Консольный продюсер Kafka не подходит: он читает строки, а байты 0x0a в значении ломают границы сообщений.
 */
public class Replay {

    static final String REPLAYED_FROM = "x-replayed-from";

    public static void main(String[] args) throws Exception {
        if (args.length == 3 && args[0].equals("produce")) {
            System.exit(produce(args[1], args[2]));
        } else if (args.length == 3 && args[0].equals("dump")) {
            dump(args[1], args[2]);
        } else {
            System.err.println("использование: produce BOOTSTRAP КЛЮЧ_ОБЪЕКТА < объект.json | dump BOOTSTRAP ТЕМА");
            System.exit(1);
        }
    }

    static int produce(String bootstrap, String objectKey) throws Exception {
        JsonNode doc = new ObjectMapper().readTree(System.in);
        if (doc == null || !doc.isObject()) {
            System.err.println("тело объекта не JSON");
            return 1;
        }
        if ("unreadable".equals(text(doc, "kind"))) {
            System.err.println("нечитаемая запись failed-events (kind = unreadable): в объекте её собственные байты, "
                    + "исходного сообщения и темы нет - возвращать нечего");
            return 3;
        }
        String topic = text(doc, "sourceTopic");
        if (topic == null || topic.isEmpty()) {
            System.err.println("в объекте нет sourceTopic");
            return 1;
        }

        byte[] key = bytes(doc, "key");
        byte[] value = bytes(doc, "value");
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, null, key, value);
        JsonNode headers = doc.get("headers");
        if (headers != null && headers.isObject()) {
            for (Map.Entry<String, JsonNode> e : headers.properties()) {
                if (!REPLAYED_FROM.equals(e.getKey()) && !e.getValue().isNull()) {
                    record.headers().add(e.getKey(), Base64.getDecoder().decode(e.getValue().asText()));
                }
            }
        }
        record.headers().add(REPLAYED_FROM, objectKey.getBytes(StandardCharsets.UTF_8));

        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        cfg.put(ProducerConfig.ACKS_CONFIG, "all");
        cfg.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        cfg.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        cfg.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 20000);
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(cfg)) {
            RecordMetadata md = producer.send(record).get(25, TimeUnit.SECONDS);
            System.out.println("возвращено: тема " + md.topic() + ", раздел " + md.partition() + ", смещение " + md.offset());
        }
        return 0;
    }

    static void dump(String bootstrap, String topic) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(cfg)) {
            List<TopicPartition> partitions = new ArrayList<>();
            consumer.partitionsFor(topic).forEach(p -> partitions.add(new TopicPartition(topic, p.partition())));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            long deadline = System.currentTimeMillis() + 20000;
            while (System.currentTimeMillis() < deadline && !reached(consumer, end)) {
                for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(500))) {
                    StringBuilder line = new StringBuilder();
                    line.append("partition=").append(r.partition())
                            .append(" key=").append(hex(r.key()))
                            .append(" value=").append(hex(r.value()));
                    for (Header h : r.headers()) {
                        line.append(" h.").append(h.key()).append('=').append(hex(h.value()));
                    }
                    System.out.println(line);
                }
            }
        }
    }

    static boolean reached(KafkaConsumer<byte[], byte[]> consumer, Map<TopicPartition, Long> end) {
        for (Map.Entry<TopicPartition, Long> e : end.entrySet()) {
            if (consumer.position(e.getKey()) < e.getValue()) {
                return false;
            }
        }
        return true;
    }

    static String text(JsonNode doc, String field) {
        JsonNode n = doc.get(field);
        return n == null || n.isNull() ? null : n.asText();
    }

    /** base64 поля объекта в байты; отсутствие (null) - null: у сообщения не было ключа или значения. */
    static byte[] bytes(JsonNode doc, String field) {
        String v = text(doc, field);
        return v == null ? null : Base64.getDecoder().decode(v);
    }

    /** Пустое место (нет ключа, нет значения) отличается от пустых байт: «-» и пустая строка. */
    static String hex(byte[] b) {
        if (b == null) {
            return "-";
        }
        StringBuilder sb = new StringBuilder();
        for (byte x : b) {
            sb.append(String.format("%02x", x));
        }
        return sb.toString();
    }
}
