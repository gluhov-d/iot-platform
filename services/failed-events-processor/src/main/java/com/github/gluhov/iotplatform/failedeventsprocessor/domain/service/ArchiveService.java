package com.github.gluhov.iotplatform.failedeventsprocessor.domain.service;

import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.ArchiveCoordinates;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.ArchiveKey;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.FailedRecord;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.HeaderEntry;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.PutOutcome;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.model.UnreadableRecord;
import com.github.gluhov.iotplatform.failedeventsprocessor.domain.port.out.FailedEventArchive;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Сборка объекта ошибки: ключ, JSON-документ и метаданные. Документ собирается явно из полей записи:
 * байты - base64, время - ISO-8601 UTC. Avro-запись целиком не сериализуется.
 */
public class ArchiveService {

    /** Версия документа: 2 - заголовки массивом [{name, value}] в порядке записи; версия 1 читают Replay.java и прежние объекты. */
    private static final int SCHEMA_VERSION = 2;
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private final FailedEventArchive archive;

    public ArchiveService(FailedEventArchive archive) {
        this.archive = archive;
    }

    public PutOutcome archive(FailedRecord record, ArchiveCoordinates coordinates) {
        return archive.put(ArchiveKey.of(coordinates), document(record, coordinates), metadata(record));
    }

    /** Нечитаемая запись failed-events: исходные байты в base64, причина и время записи (FR-010); ключ тот же, что у разобранной записи. */
    public PutOutcome archiveUnreadable(UnreadableRecord record) {
        return archive.put(ArchiveKey.of(record), unreadableDocument(record),
                Map.of("kind", "unreadable"));
    }

    private static byte[] unreadableDocument(UnreadableRecord record) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("schemaVersion", SCHEMA_VERSION);
        doc.put("kind", "unreadable");
        doc.put("reason", record.reason());
        doc.put("failedEventsTopic", record.topic());
        doc.put("failedEventsPartition", record.partition());
        doc.put("failedEventsOffset", record.offset());
        doc.put("recordTimestamp", record.recordTimestamp().toString());
        doc.put("key", base64(record.key()));
        doc.put("value", base64(record.value()));
        doc.put("headers", headerArray(record.headers()));
        return JSON.writeValueAsBytes(doc);
    }

    private static byte[] document(FailedRecord record, ArchiveCoordinates coordinates) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("schemaVersion", SCHEMA_VERSION);
        doc.put("sourceService", record.sourceService());
        doc.put("sourceTopic", record.sourceTopic());
        doc.put("sourcePartition", record.sourcePartition());
        doc.put("sourceOffset", record.sourceOffset());
        doc.put("sourceTimestamp", record.sourceTimestamp().toString());
        doc.put("failedAt", record.failedAt().toString());
        doc.put("errorCode", record.errorCode());
        doc.put("errorType", record.errorType());
        doc.put("description", record.description());
        doc.put("attempts", record.attempts());
        doc.put("traceId", record.traceId());
        doc.put("deviceId", record.deviceId());
        doc.put("key", base64(record.key()));
        doc.put("value", base64(record.value()));
        doc.put("headers", headerArray(record.headers()));
        Map<String, Object> archiveBlock = new LinkedHashMap<>();
        archiveBlock.put("failedEventsTopic", coordinates.topic());
        archiveBlock.put("failedEventsPartition", coordinates.partition());
        archiveBlock.put("failedEventsOffset", coordinates.offset());
        archiveBlock.put("recordTimestamp", coordinates.recordTimestamp().toString());
        archiveBlock.put("archivedAt", coordinates.archivedAt().toString());
        doc.put("archive", archiveBlock);
        return JSON.writeValueAsBytes(doc);
    }

    /** Метаданные объекта для поиска без чтения тела; ключи без префикса x-amz-meta- (его добавляет SDK). */
    private static Map<String, String> metadata(FailedRecord record) {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("error-code", record.errorCode());
        metadata.put("source-service", record.sourceService());
        metadata.put("source-topic", record.sourceTopic());
        if (record.traceId() != null) {
            metadata.put("trace-id", record.traceId());
        }
        if (record.deviceId() != null) {
            metadata.put("device-id", record.deviceId());
        }
        return metadata;
    }

    /** Заголовки документа: массив {name, value} в порядке записи, с повторами; value - base64 или null. */
    private static List<Map<String, Object>> headerArray(List<HeaderEntry> headers) {
        List<Map<String, Object>> array = new ArrayList<>();
        if (headers != null) {
            for (HeaderEntry header : headers) {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("name", header.name());
                entry.put("value", base64(header.value()));
                array.add(entry);
            }
        }
        return array;
    }

    private static String base64(byte[] bytes) {
        return bytes == null ? null : Base64.getEncoder().encodeToString(bytes);
    }
}
