package io.heddle.transaction;

import io.heddle.api.Owned;
import io.heddle.api.Stage;
import io.heddle.api.StageContext;
import io.heddle.api.TransactionalStage;

/**
 * Abstract base class for pipeline stages that wrap a unit of work with commit and
 * rollback semantics.
 *
 * <p>Items accumulate across calls to {@link #process(Owned, StageContext)}, building
 * up a transactional unit of work in memory. When the upstream signals end-of-stream,
 * the pipeline runtime calls {@link Stage#flush(StageContext)}, which delegates to
 * {@link #commit()} to persist the accumulated work atomically. If any exception occurs
 * before the flush phase, {@link #rollback(Throwable)} is called to undo or discard
 * partial state.
 *
 * <p>Typical use cases include JDBC batch inserts, atomic file renames, and any
 * operation that must succeed or fail as a single unit across a set of pipeline items:
 *
 * <pre>{@code
 * public class JdbcBatchStage extends TransactionStage<Record, Record> {
 *
 *     private final Connection conn;
 *     private PreparedStatement stmt;
 *
 *     @Override
 *     public void process(Owned<Record> item, StageContext<Record> ctx) throws Exception {
 *         if (stmt == null) stmt = conn.prepareStatement("INSERT INTO t VALUES (?)");
 *         Record record = item.consume();
 *         stmt.setString(1, record.name());
 *         stmt.addBatch();
 *         ctx.emit(record);
 *     }
 *
 *     @Override
 *     public void commit() throws Exception {
 *         if (stmt != null) { stmt.executeBatch(); conn.commit(); }
 *     }
 *
 *     @Override
 *     public void rollback(Throwable cause) {
 *         try { conn.rollback(); } catch (Exception e) { /* ignored * / }
 *     }
 * }
 * }</pre>
 *
 * @param <I> the input element type consumed by this stage
 * @param <O> the output element type emitted by this stage
 * @see TransactionalStage
 * @see Stage
 */
public abstract class TransactionStage<I, O> implements Stage<I, O>, TransactionalStage {

    /**
     * Processes one item, optionally accumulating state toward the transactional unit
     * of work and emitting zero or more output items downstream.
     *
     * @param item the owned input item; must be consumed exactly once via
     *             {@link Owned#consume()}
     * @param ctx  the emission context used to send output items downstream
     */
    @Override
    public abstract void process(Owned<I> item, StageContext<O> ctx);

    /**
     * Commits the accumulated unit of work, persisting it atomically.
     *
     * <p>Called once by the pipeline runtime after the last item has been successfully
     * processed and {@link Stage#flush(StageContext)} is invoked. Implementations should
     * make the accumulated state durable here. Any exception thrown is wrapped in a
     * {@link io.heddle.error.HeddleException} and treated as a pipeline failure.
     *
     * @throws Exception if the commit operation fails
     */
    public abstract void commit() throws Exception;

    /**
     * Rolls back or discards any partially accumulated state.
     *
     * <p>Called by the pipeline runtime when a failure occurs before {@link #commit()}
     * is reached. Implementations should release all resources and undo any side-effects
     * that were not yet committed. If this method itself throws, the exception is added
     * as a suppressed cause on the original pipeline failure.
     *
     * @param cause the exception that triggered the rollback
     */
    @Override
    public abstract void rollback(Throwable cause);

    /**
     * Delegates to {@link #commit()}, wrapping any checked exception in a
     * {@link io.heddle.error.HeddleException}.
     *
     * @param ctx the emission context; unused by this implementation but available to
     *            subclasses that override this method
     */
    @Override
    public void flush(StageContext<O> ctx) {
        try {
            commit();
        } catch (Exception e) {
            throw new io.heddle.error.HeddleException("transaction commit failed", e);
        }
    }
}
