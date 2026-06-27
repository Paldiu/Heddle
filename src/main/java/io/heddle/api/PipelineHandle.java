package io.heddle.api;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Control handle returned by pipeline terminal operators.
 *
 * <p>A {@code PipelineHandle} is obtained by calling a terminal operator such as
 * {@link io.heddle.Pipeline#sink(Sink)}, {@link io.heddle.Heddle#wire(PipelineDefinition)},
 * or {@link io.heddle.Heddle#run(PipelineDefinition)}. Pipelines do not start
 * automatically: call {@link #start()} to begin execution. The blocking convenience
 * terminals such as {@link io.heddle.Pipeline#toList()} and
 * {@link io.heddle.Pipeline#forEach(Consumer)} start and await the pipeline internally.
 *
 * <p>A pipeline progresses through the following states:
 * <ol>
 *   <li><em>Assembled</em>: constructed but not yet started ({@link #isRunning()} returns
 *       {@code false})</li>
 *   <li><em>Running</em>: active after {@link #start()} is called</li>
 *   <li><em>Terminal</em>: one of complete, cancelled, or failed</li>
 * </ol>
 *
 * <pre>{@code
 * PipelineHandle handle = Heddle.fromLines(path)
 *         .map(String::trim)
 *         .sink(db::insert);
 *
 * handle.onComplete(() -> System.out.println("done"))
 *       .onError(ex -> log.error("failed", ex))
 *       .start();
 *
 * handle.awaitCompletion(Duration.ofMinutes(5));
 * }</pre>
 *
 * @see io.heddle.Pipeline#sink(Sink)
 * @see io.heddle.Heddle#wire(PipelineDefinition)
 */
public interface PipelineHandle {

    /**
     * Starts the pipeline, launching all stage virtual threads.
     *
     * <p>This method returns immediately; the pipeline runs concurrently on its own
     * virtual threads. Call {@link #awaitCompletion()} to block the caller until the
     * pipeline finishes.
     *
     * @return this handle, for method chaining
     */
    PipelineHandle start();

    /**
     * Requests a graceful shutdown of the pipeline.
     *
     * <p>The source stage stops emitting new items, but items already in transit
     * continue through the stage graph and are delivered to the sink. The pipeline
     * completes normally once all in-flight items have drained.
     */
    void stop();

    /**
     * Cancels the pipeline immediately by interrupting all stage virtual threads.
     *
     * <p>In-flight items are discarded. The pipeline transitions to the cancelled
     * terminal state as soon as all stage threads acknowledge the interrupt.
     */
    void cancel();

    /**
     * Blocks the calling thread until the pipeline reaches a terminal state: complete,
     * cancelled, or failed.
     */
    void awaitCompletion();

    /**
     * Blocks the calling thread until the pipeline reaches a terminal state, or until
     * the specified timeout elapses.
     *
     * @param timeout the maximum duration to wait
     * @throws TimeoutException if the pipeline has not reached a terminal state within
     *                          the given timeout
     */
    void awaitCompletion(Duration timeout) throws TimeoutException;

    /**
     * Returns {@code true} if the pipeline has been started and has not yet reached
     * a terminal state.
     *
     * @return {@code true} if the pipeline is currently executing
     */
    boolean isRunning();

    /**
     * Returns {@code true} if the pipeline completed successfully, meaning all items
     * were processed and the sink received its {@link Sink#onComplete()} callback.
     *
     * @return {@code true} if the pipeline completed normally
     */
    boolean isComplete();

    /**
     * Returns {@code true} if the pipeline was cancelled via {@link #cancel()}.
     *
     * @return {@code true} if the pipeline was cancelled before it could complete
     */
    boolean isCancelled();

    /**
     * Returns {@code true} if the pipeline terminated due to an unhandled exception.
     *
     * @return {@code true} if the pipeline failed; the originating cause is available
     *         via {@link #failureCause()}
     */
    boolean isFailed();

    /**
     * Returns the exception that caused the pipeline to fail, if any.
     *
     * @return an {@code Optional} containing the failure cause if {@link #isFailed()}
     *         returns {@code true}; an empty {@code Optional} otherwise
     */
    Optional<Throwable> failureCause();

    /**
     * Registers a callback to be invoked when the pipeline completes successfully.
     *
     * <p>If the pipeline has already completed normally by the time this method is
     * called, the callback is invoked immediately. Multiple callbacks may be registered;
     * they are invoked in registration order.
     *
     * @param callback the callback to invoke on normal completion
     * @return this handle, for method chaining
     */
    PipelineHandle onComplete(Runnable callback);

    /**
     * Registers a callback to be invoked when the pipeline fails.
     *
     * <p>If the pipeline has already failed by the time this method is called, the
     * callback is invoked immediately. Multiple callbacks may be registered; they are
     * invoked in registration order.
     *
     * @param callback the callback to invoke on pipeline failure; receives the
     *                 originating exception
     * @return this handle, for method chaining
     */
    PipelineHandle onError(Consumer<Throwable> callback);

    /**
     * Returns a point-in-time snapshot of the pipeline's metrics.
     *
     * <p>The snapshot includes per-stage channel occupancy, the total number of items
     * delivered to the terminal sink, elapsed wall-clock time since start, and the
     * current terminal state. This method is safe to call from any thread at any time,
     * including before {@link #start()} is called and after the pipeline has completed.
     *
     * @return a {@link PipelineStats} snapshot reflecting the pipeline's state at
     *         the time of this call
     */
    PipelineStats stats();
}
