package com.github.gluhov.iotplatform.devicecollector.adapter.out.postgres;

import com.github.gluhov.iotplatform.devicecollector.domain.exception.StorageAccessException;
import com.github.gluhov.iotplatform.devicecollector.domain.model.StorageFailureReason;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JdbcTransactionRunnerTest {
    private final PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
    private final JdbcTransactionRunner runner = new JdbcTransactionRunner(transactionManager);

    @Test
    @DisplayName("Shard down: rollback fails with 'Connection is closed', the original connection error is kept")
    void givenRollbackFailure_whenWorkFailed_thenOriginalErrorRethrown() {
        when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
        doThrow(new TransactionSystemException("Could not roll back JDBC transaction",
                new SQLException("Connection is closed"))).when(transactionManager).rollback(any());
        StorageAccessException original = new StorageAccessException(StorageFailureReason.CONNECTION,
                "Select device for update failed: HikariPool-2 - Connection is not available", null);

        assertThatThrownBy(() -> runner.inTransaction(() -> {
            throw original;
        })).isSameAs(original);
    }

    @Test
    @DisplayName("Proxy unreachable at BEGIN: connection failure")
    void givenNoConnection_whenBegin_thenConnectionFailure() {
        when(transactionManager.getTransaction(any())).thenThrow(new CannotCreateTransactionException(
                "Could not open JDBC Connection", new SQLTransientConnectionException("sharding-proxy - Connection is not available")));

        assertThatThrownBy(() -> runner.inTransaction(() -> "never"))
                .isInstanceOfSatisfying(StorageAccessException.class,
                        e -> assertThat(e.reason()).isEqualTo(StorageFailureReason.CONNECTION));
    }

    @Test
    @DisplayName("COMMIT failed: classified by its SQLState")
    void givenCommitFailure_whenWorkDone_thenClassified() {
        when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
        doThrow(new TransactionSystemException("Could not commit JDBC transaction",
                new SQLException("I/O error", "08006"))).when(transactionManager).commit(any());

        assertThatThrownBy(() -> runner.inTransaction(() -> "done"))
                .isInstanceOfSatisfying(StorageAccessException.class,
                        e -> assertThat(e.reason()).isEqualTo(StorageFailureReason.CONNECTION));
    }

    @Test
    @DisplayName("Successful work: result returned")
    void givenWork_whenCommitted_thenResult() {
        when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());

        assertThat(runner.inTransaction(() -> "shard_0")).isEqualTo("shard_0");
    }
}