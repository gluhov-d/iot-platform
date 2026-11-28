package com.github.gluhov.iotplatform.failedeventsprocessor.it;

import com.github.gluhov.iotplatform.events.avro.FailedEvent;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import tools.jackson.databind.JsonNode;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Недоступность MinIO и Schema Registry (FR-009, FR-010; R6, R7). Паузы - docker pause: адрес и порт не меняются.
 * Пауза MinIO дольше max.poll.interval.ms (30 с): группа должна остаться Stable за счёт тайм-аутов клиента и паузы контейнера.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ItFailedEventsOutageTest extends AbstractItTest {

    private static final Duration MINIO_PAUSE = Duration.ofSeconds(45);
    private static final Duration REGISTRY_PAUSE = Duration.ofSeconds(20);
    /** Больше FAILED_EVENTS_MAX_POLL_INTERVAL_MS (30 с): зависший Registry не должен выбивать потребителя из группы (R25). */
    private static final Duration REGISTRY_HUNG_PAUSE = Duration.ofSeconds(40);

    @Test
    @Order(1)
    void givenMinioPausedLongerThanMaxPollInterval_whenProcessed_thenGroupStableAndObjectAfterUnpause() throws Exception {
        long offset = nextSourceOffset();
        long committedBefore = committedOffset();
        pause(MINIO);
        RecordMetadata sent;
        try {
            sent = send(FAILED + "-" + offset, event(offset, null, Instant.parse("2026-11-21T10:15:31.480Z")));
            Instant until = Instant.now().plus(MINIO_PAUSE);
            while (Instant.now().isBefore(until)) {
                Thread.sleep(5_000);
                assertThat(groupState()).as("группа Stable во время паузы MinIO").isEqualToIgnoringCase("Stable");
                assertThat(committedOffset()).as("смещение не движется во время паузы").isEqualTo(committedBefore);
            }
        } finally {
            unpause(MINIO);
        }

        String key = objectKey(sent);
        awaitObject(key);
        awaitCommitted(sent);
        assertThat(readJson(key).get("sourceOffset").asLong()).isEqualTo(offset);
    }

    @Test
    @Order(2)
    void givenRegistryPaused_whenProcessed_thenNoUnreadableWhileDownAndUnreadableAfterUnpause() throws Exception {
        // Схема 99999 не зарегистрирована: при работающем Schema Registry - 404, то есть нечитаемая запись.
        // Пауза ставится ДО отправки: иначе запись могла бы разобраться раньше паузы.
        RecordMetadata sent;
        long committedBefore = committedOffset();
        pause(SCHEMA_REGISTRY);
        try {
            sent = sendRaw("sr-down-" + nextSourceOffset(), new byte[]{0, 0, 0, 0x27, 0x0F, 1, 2, 3});
            String key = objectKey(sent);
            Instant until = Instant.now().plus(REGISTRY_PAUSE);
            while (Instant.now().isBefore(until)) {
                Thread.sleep(5_000);
                assertThat(groupState()).as("группа Stable во время паузы Schema Registry").isEqualToIgnoringCase("Stable");
                assertThat(committedOffset()).as("смещение не движется во время паузы Schema Registry")
                        .isEqualTo(committedBefore);
                assertThat(exists(key)).as("во время паузы Schema Registry unreadable не создан").isFalse();
            }
        } finally {
            unpause(SCHEMA_REGISTRY);
        }

        String key = objectKey(sent);
        awaitObject(key);
        awaitCommitted(sent);
        JsonNode doc = readJson(key);
        assertThat(doc.get("kind").asString()).isEqualTo("unreadable");
        assertThat(doc.get("reason").asString()).contains("RestClientException");
    }

    /**
     * Registry завис дольше max.poll.interval.ms: клиент получает тайм-аут (временная ошибка, без повторов), группа остаётся
     * Stable, смещение не движется, объекта нет. После снятия паузы запись разбирается штатно.
     * Запись - с id схемы, которого потребитель ещё не запрашивал (вариант схемы с лишним полем): иначе id уже в кеше и Registry не нужен.
     */
    @Test
    @Order(3)
    void givenRegistryHungLongerThanMaxPollInterval_whenProcessed_thenGroupStableNoObjectAndParsedAfterUnpause() throws Exception {
        long offset = nextSourceOffset();
        byte[] valueBytes = variantAvroBytes(event(offset, null, Instant.parse("2026-11-21T10:15:31.480Z")));
        long committedBefore = committedOffset();
        RecordMetadata sent;
        String key;
        pause(SCHEMA_REGISTRY);
        try {
            sent = sendRaw("hung-" + offset, valueBytes);
            key = objectKey(sent);
            Instant until = Instant.now().plus(REGISTRY_HUNG_PAUSE);
            while (Instant.now().isBefore(until)) {
                Thread.sleep(5_000);
                assertThat(groupState()).as("группа Stable во время зависания Schema Registry").isEqualToIgnoringCase("Stable");
                assertThat(committedOffset()).as("смещение не движется во время зависания").isEqualTo(committedBefore);
                assertThat(exists(key)).as("во время зависания объект не создан").isFalse();
            }
        } finally {
            unpause(SCHEMA_REGISTRY);
        }

        awaitObject(key);
        awaitCommitted(sent);
        JsonNode doc = readJson(key);
        assertThat(doc.get("kind")).as("запись разобрана штатно, не unreadable").isNull();
        assertThat(doc.get("sourceOffset").asLong()).isEqualTo(offset);
    }

    /**
     * Байты записи FailedEvent в формате Avro с id варианта схемы: лишнее необязательное поле в конце записи делает каноническую
     * форму схемы другой (doc в неё не входит и id не менял бы), поэтому id новый, а читатель разбирает запись по своей схеме.
     * Регистрация идёт до паузы Registry и под другим subject, чтобы не менять версии failed-events-value; производитель
     * отправляет готовые байты, которым Registry не нужен.
     */
    private static byte[] variantAvroBytes(FailedEvent event) throws Exception {
        Schema base = FailedEvent.getClassSchema();
        String baseJson = base.toString();
        int fieldsEnd = baseJson.lastIndexOf("]");
        Schema variant = new Schema.Parser().parse(baseJson.substring(0, fieldsEnd)
                + ",{\"name\":\"hangProbe\",\"type\":[\"null\",\"string\"],\"default\":null}"
                + baseJson.substring(fieldsEnd));
        int id;
        try (var client = new CachedSchemaRegistryClient(schemaRegistryUrl(), 10)) {
            id = client.register(FAILED + "-hang-probe-value", new AvroSchema(variant));
        }
        // Формат Confluent: байт 0, id схемы (4 байта, big-endian), затем Avro-тело по варианту схемы
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0);
        out.write(ByteBuffer.allocate(4).putInt(id).array());
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new SpecificDatumWriter<FailedEvent>(base).write(event, encoder);
        // Поле hangProbe = null: индекс ветки union 0 (zigzag-кодирование - один нулевой байт)
        encoder.writeIndex(0);
        encoder.flush();
        return out.toByteArray();
    }
}
