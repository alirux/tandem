package com.codingful.tandem.spring.producer;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * A real, resource-less transaction manager for the no-database runtime tests: it opens and closes a
 * transaction scope and marks it active exactly as a JDBC manager would, so the write-side tiers' "must run
 * inside a transaction" guards see the truth and Spring's own {@code @Transactional} interception has a
 * manager to drive.
 *
 * <p>It manages no resource, so it deliberately cannot demonstrate that a rollback discards outbox rows —
 * that is what the Testcontainers integration tests are for.
 */
final class StubTransactionManager implements PlatformTransactionManager {

    @Override
    public TransactionStatus getTransaction(TransactionDefinition definition) {
        TransactionSynchronizationManager.setActualTransactionActive(true);
        return new SimpleTransactionStatus(true);
    }

    @Override
    public void commit(TransactionStatus status) {
        TransactionSynchronizationManager.setActualTransactionActive(false);
    }

    @Override
    public void rollback(TransactionStatus status) {
        TransactionSynchronizationManager.setActualTransactionActive(false);
    }
}
