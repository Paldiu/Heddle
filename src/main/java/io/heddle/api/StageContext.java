package io.heddle.api;

/**
 * Emission handle supplied to each pipeline stage during item processing.
 *
 * <p>A {@code StageContext} is the sole mechanism by which a {@link Stage} delivers
 * output items to the next stage in the pipeline. There is no separate release or
 * acknowledgement step: calling {@link #emit(Object)} both delivers the item and
 * signals that it is ready for downstream consumption.
 *
 * <p>{@link #emit(Object)} may block when the downstream channel is full. This
 * blocking is the backpressure mechanism: the producing virtual thread parks until
 * the consumer catches up, preventing unbounded queue growth without sacrificing
 * throughput.
 *
 * @param <O> the output element type accepted by this context
 * @see Stage
 * @see io.heddle.policies.BackpressurePolicy
 */
@FunctionalInterface
public interface StageContext<O> {

    /**
     * Delivers one item to the downstream pipeline stage.
     *
     * <p>This method may block if the downstream channel is at capacity. The calling
     * virtual thread parks until a slot is available, releasing its carrier thread
     * without blocking a platform thread.
     *
     * @param value the item to deliver downstream
     */
    void emit(O value);

    /**
     * Delivers all items in the given iterable to the downstream stage, in iteration
     * order.
     *
     * <p>Each item is delivered via a separate call to {@link #emit(Object)}; the same
     * backpressure semantics apply to each individual emission.
     *
     * @param values the items to deliver; each element is emitted in encounter order
     */
    default void emitAll(Iterable<? extends O> values) {
        for (O v : values) emit(v);
    }
}
