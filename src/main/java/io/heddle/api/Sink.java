package io.heddle.api;

/**
 * Terminal consumer that receives items from the final stage of a pipeline.
 *
 * <p>For simple consumption with no lifecycle requirements, prefer passing a plain
 * {@link java.util.function.Consumer Consumer}{@code <T>} to
 * {@link io.heddle.Pipeline#sink(java.util.function.Consumer)}. Implement
 * {@code Sink} directly when you need to react to pipeline lifecycle events:
 * {@link #onComplete()} is called after the last item is delivered, and
 * {@link #onError(Throwable)} is called if the pipeline fails before it drains.
 *
 * <p>{@code Sink} is a functional interface: a lambda or method reference may be
 * used wherever only item consumption is required and the lifecycle callbacks are
 * not needed.
 *
 * <pre>{@code
 * // Simple lambda sink (no lifecycle hooks)
 * pipeline.sink(System.out::println).start();
 *
 * // Full Sink with lifecycle hooks
 * pipeline.sink(new Sink<String>() {
 *     @Override public void accept(String item)  { db.insert(item); }
 *     @Override public void onComplete()          { db.commit(); }
 *     @Override public void onError(Throwable t) { db.rollback(); }
 * }).start();
 * }</pre>
 *
 * @param <T> the type of items accepted by this sink
 * @see io.heddle.Pipeline#sink(Sink)
 * @see DeadLetterSink
 */
@FunctionalInterface
public interface Sink<T> {

    /**
     * Accepts one item delivered from the final pipeline stage.
     *
     * @param item the item to consume
     */
    void accept(T item);

    /**
     * Called once, by the pipeline runtime, after the last item has been successfully
     * delivered to this sink.
     *
     * <p>Use this method to commit accumulated work, flush buffers, or release
     * resources that are only safe to release after all items have been processed.
     * The default implementation is a no-op.
     */
    default void onComplete() {}

    /**
     * Called if the pipeline terminates with a failure before it drains completely.
     *
     * <p>The number of items already delivered to {@link #accept(Object)} before this
     * callback is invoked depends on which stage failed and on the configured
     * {@link io.heddle.error.ErrorStrategy}. Use this method to roll back partial work
     * or log the failure. The default implementation is a no-op.
     *
     * @param cause the exception that caused the pipeline to fail
     */
    default void onError(Throwable cause) {}
}
