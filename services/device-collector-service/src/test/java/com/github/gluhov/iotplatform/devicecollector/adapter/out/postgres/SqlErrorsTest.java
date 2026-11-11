package com.github.gluhov.iotplatform.devicecollector.adapter.out.postgres;

import com.github.gluhov.iotplatform.devicecollector.domain.exception.StorageAccessException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StorageFailureReason;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.transaction.CannotCreateTransactionException;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;

import static org.assertj.core.api.Assertions.assertThat;

/** SQLState - как их возвращает ShardingSphere-Proxy 5.5.3 (проверено на стенде). */
class SqlErrorsTest {

    @ParameterizedTest(name = "SQLState {0} -> {1}")
    @CsvSource({
            "08003, CONNECTION",   // шард остановлен: пул Proxy не дал соединение за 3 с
            "08001, CONNECTION",   // Proxy остановлен
            "08006, CONNECTION",   // соединение оборвалось
            "57014, TIMEOUT",      // statement_timeout на шарде
            "55P03, TIMEOUT",      // lock_timeout на шарде
            "44000, ROUTING",      // запрос без ключа шардирования
            "42S02, ROUTING",      // таблица или колонка неизвестна Proxy
            "23505, OTHER"         // конфликт ключа
    })
    @DisplayName("SQLState from Proxy is mapped to a failure reason")
    void givenSqlState_whenClassify_thenReason(String sqlState, StorageFailureReason expected) {
        RuntimeException e = new UncategorizedSQLException("query", "SELECT 1", new SQLException("failed", sqlState));

        assertThat(SqlErrors.classify(e)).isEqualTo(expected);
    }

    @Test
    @DisplayName("Own connection pool timeout (Proxy does not answer) is a connection failure")
    void givenPoolTimeout_whenClassify_thenConnection() {
        assertThat(SqlErrors.classify(new CannotCreateTransactionException("Could not open JDBC Connection",
                new SQLTransientConnectionException("sharding-proxy - Connection is not available")))).isEqualTo(StorageFailureReason.CONNECTION);
        assertThat(SqlErrors.classify(new CannotGetJdbcConnectionException("no connection")))
                .isEqualTo(StorageFailureReason.CONNECTION);
        assertThat(SqlErrors.classify(new DataAccessResourceFailureException("timeout", new SQLTimeoutException("timeout"))))
                .isEqualTo(StorageFailureReason.TIMEOUT);
    }

    @Test
    @DisplayName("Storage exception keeps the operation, the root message and the cause")
    void givenDataAccessException_whenConvert_thenStorageExceptionWithRootMessage() {
        DuplicateKeyException cause = new DuplicateKeyException("insert",
                new SQLException("duplicate key value violates unique constraint\n  Detail: ...", "23505"));

        StorageAccessException converted = SqlErrors.toStorageException("Insert devices", cause);

        assertThat(converted.reason()).isEqualTo(StorageFailureReason.OTHER);
        assertThat(converted.getMessage()).startsWith("Insert devices failed: duplicate key").doesNotContain("\n");
        assertThat(converted.getCause()).isSameAs(cause);
    }
}