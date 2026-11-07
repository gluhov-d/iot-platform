package com.github.gluhov.iotplatform.devicecollector.adapter.out.postgres;

import com.github.gluhov.iotplatform.devicecollector.domain.exception.StorageAccessException;
import com.github.gluhov.iotplatform.devicecollector.domain.port.out.TransactionRunner;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.function.Supplier;

/** Транзакция на соединении с Proxy: все запросы внутри - по одному device_id, Proxy ведёт её на одном шарде. */
@Component
public class JdbcTransactionRunner implements TransactionRunner {
    private final TransactionTemplate transactions;

    public JdbcTransactionRunner(PlatformTransactionManager transactionManager) {
        this.transactions = new TransactionTemplate(transactionManager);
    }

    @Override
    public <T> T inTransaction(Supplier<T> work) {
        try {
            return transactions.execute(status -> work.get());
        } catch (StorageAccessException e) {
            throw e;
        } catch (TransactionSystemException e) {
            // Шард недоступен: Proxy вернул 08003, пул закрыл соединение, и ROLLBACK упал с "Connection is closed"
            // без SQLState. TransactionTemplate подменяет исходную ошибку ошибкой отката - причину берём исходную
            if (e.getApplicationException() instanceof StorageAccessException original) {
                throw original;
            }
            throw SqlErrors.toStorageException("Transaction", e);
        } catch (TransactionException | DataAccessException e) {
            // Соединение не получено (CannotCreateTransactionException) или COMMIT не прошёл
            throw SqlErrors.toStorageException("Transaction", e);
        }
    }
}