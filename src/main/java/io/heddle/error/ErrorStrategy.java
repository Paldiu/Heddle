package io.heddle.error;

import io.heddle.api.DeadLetterSink;

/**
 * Per-item error handling strategy applied when a pipeline stage throws an exception.
 *
 * <p>{@code ErrorStrategy} is a sealed interface with four permitted implementations,
 * allowing the runtime to switch exhaustively over all outcomes. Attach a strategy to
 * a stage via {@link io.heddle.Pipeline#onError(ErrorStrategy)}:
 *
 * <pre>{@code
 * pipeline.map(riskyTransform)
 *         .onError(new ErrorStrategy.Skip())
 *         .filter(isValid)
 *         .sink(sink);
 * }</pre>
 *
 * <p>Failed items never travel the main data path. The {@link DeadLetter} strategy
 * routes them to a side channel to prevent queue drag on the main pipeline.
 *
 * @see io.heddle.Pipeline#onError(ErrorStrategy)
 * @see io.heddle.Pipeline#retry(int)
 * @see DeadLetterSink
 */
public sealed interface ErrorStrategy
        permits ErrorStrategy.Stop,
                ErrorStrategy.Skip,
                ErrorStrategy.Retry,
                ErrorStrategy.DeadLetter {

    /**
     * Escalates the failure by signalling a pipeline failure and halting all processing.
     *
     * <p>This is the default strategy applied to every stage unless explicitly
     * overridden with {@link io.heddle.Pipeline#onError(ErrorStrategy)}.
     */
    record Stop() implements ErrorStrategy {}

    /**
     * Silently drops the failed item and continues processing subsequent items normally.
     *
     * <p>No exception is recorded and the pipeline does not fail. Use this strategy
     * when individual item failures are expected and acceptable.
     */
    record Skip() implements ErrorStrategy {}

    /**
     * Re-attempts processing of the failed item up to {@code max} times before falling
     * through to {@link Stop} on exhaustion.
     *
     * @param max the maximum number of retry attempts; must be positive
     */
    record Retry(int max) implements ErrorStrategy {}

    /**
     * Routes the failed item and its cause to a {@link DeadLetterSink}, then continues
     * processing subsequent items normally.
     *
     * <p>This strategy prevents individual item failures from halting the pipeline
     * while preserving full context for each failed item. Prefer the
     * {@link #deadLetterTo(DeadLetterSink)} factory over constructing this record
     * directly, as the factory provides better type inference.
     *
     * @param sink the sink that receives each failed item and its cause
     */
    record DeadLetter(DeadLetterSink<?> sink) implements ErrorStrategy {}

    /**
     * Creates a {@link DeadLetter} strategy that routes failed items to the given sink.
     *
     * <p>The pipeline continues processing subsequent items after routing each failure.
     * Both the failing item and the exception are passed to the sink, enabling logging,
     * auditing, or re-queuing without halting the pipeline.
     *
     * <pre>{@code
     * List<FailedItem> dlq = new CopyOnWriteArrayList<>();
     * pipeline.map(riskyFn)
     *         .onError(ErrorStrategy.deadLetterTo(
     *             (item, cause) -> dlq.add(new FailedItem(item, cause))))
     *         .sink(sink);
     * }</pre>
     *
     * @param <T>  the type of items produced by the failing stage
     * @param sink the sink that receives each failed item and its cause;
     *             must not be {@code null}
     * @return a {@link DeadLetter} strategy backed by the given sink
     */
    static <T> DeadLetter deadLetterTo(DeadLetterSink<T> sink) {
        return new DeadLetter(sink);
    }
}
