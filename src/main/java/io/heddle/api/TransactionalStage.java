package io.heddle.api;

/**
 * Opt-in protocol for pipeline stages that manage a transactional unit of work.
 *
 * <p>The pipeline runtime calls {@link #rollback(Throwable)} when user code in the
 * stage throws an exception before {@link Stage#flush(StageContext)} completes.
 * Implementations should undo or discard any partially accumulated work: close
 * connections, delete temporary files, abort a batch write, and so on.
 *
 * <p>If {@code rollback()} itself throws, the exception is suppressed and added to
 * the original pipeline failure as a suppressed cause. Implementations should handle
 * their own rollback errors internally rather than relying on this suppression.
 *
 * <p>{@link io.heddle.transaction.TransactionStage} is the standard abstract base
 * class that provides a concrete commit/rollback framework. Custom stages may also
 * implement this interface directly without extending {@code TransactionStage}.
 *
 * @see io.heddle.transaction.TransactionStage
 * @see Stage
 */
public interface TransactionalStage {

    /**
     * Rolls back any partially accumulated work performed by this stage.
     *
     * <p>Called by the pipeline runtime when a failure occurs before the stage's flush
     * phase completes. Implementations should release all resources and undo any
     * side-effects that were not yet committed.
     *
     * @param cause the exception that triggered the rollback
     */
    void rollback(Throwable cause);
}
