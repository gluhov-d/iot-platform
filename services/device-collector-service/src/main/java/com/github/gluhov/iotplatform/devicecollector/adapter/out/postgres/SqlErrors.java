package com.github.gluhov.iotplatform.devicecollector.adapter.out.postgres;

import com.github.gluhov.iotplatform.devicecollector.domain.exception.StorageAccessException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StorageFailureReason;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.transaction.CannotCreateTransactionException;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;

/**
 * Причина ошибки по SQLState - так её видит приложение за ShardingSphere-Proxy 5.5.3 (проверено на стенде):
 * шард остановлен - 08003 через 3 с, Proxy остановлен - 08001, statement_timeout - 57014, lock_timeout - 55P03,
 * запрос без ключа шардирования - 44000, таблица или колонка неизвестна Proxy - 42S02.
 */
final class SqlErrors {

    private SqlErrors() {
    }

    static StorageAccessException toStorageException(String operation, RuntimeException e) {
        StorageFailureReason reason = classify(e);
        return new StorageAccessException(reason, operation + " failed: " + rootMessage(e), e);
    }

    static StorageFailureReason classify(Throwable exception) {
        for (Throwable e = exception; e != null; e = e.getCause() == e ? null : e.getCause()) {
            if (e instanceof SQLTimeoutException) {
                return StorageFailureReason.TIMEOUT;
            }
            if (e instanceof SQLTransientConnectionException || e instanceof SQLNonTransientConnectionException) {
                return StorageFailureReason.CONNECTION;
            }
            if (e instanceof SQLException sql && sql.getSQLState() != null) {
                StorageFailureReason reason = bySqlState(sql.getSQLState());
                if (reason != null) {
                    return reason;
                }
            }
        }
        // Соединение не получено, а SQLState нет (Proxy не отвечает)
        if (exception instanceof CannotGetJdbcConnectionException || exception instanceof CannotCreateTransactionException) {
            return StorageFailureReason.CONNECTION;
        }
        return StorageFailureReason.OTHER;
    }

    private static StorageFailureReason bySqlState(String state) {
        if (state.startsWith("08")) {
            return StorageFailureReason.CONNECTION;
        }
        return switch (state) {
            case "57014", "55P03" -> StorageFailureReason.TIMEOUT;
            case "44000", "42S02" -> StorageFailureReason.ROUTING;
            default -> null;
        };
    }

    private static String rootMessage(Throwable exception) {
        Throwable root = exception;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return String.valueOf(root.getMessage()).replace('\n', ' ');
    }
}