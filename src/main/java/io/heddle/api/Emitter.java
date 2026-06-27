package io.heddle.api;

/**
 * Push-style emission handle supplied to producer lambdas used as pipeline sources.
 *
 * <p>An {@code Emitter} is passed to the producer callback when a pipeline is
 * constructed via {@link io.heddle.Heddle#from(java.util.function.Consumer)}. The
 * producer calls {@link #emit(Object)} for each item it wants to deliver downstream
 * and simply returns when the stream is exhausted. There is no explicit end-of-stream
 * signal: the pipeline runtime treats the producer's return as the end of input.
 *
 * <p>{@code Emitter} is a functional interface. The typical usage pattern is a lambda
 * that drives the emission loop:
 *
 * <pre>{@code
 * Heddle.from((Emitter<String> emit) -> {
 *     for (String record : csvFile.readAll()) {
 *         emit.emit(record);
 *     }
 * });
 * }</pre>
 *
 * <p>The emitter is thread-confined to the source stage's virtual thread. Do not
 * pass it to other threads or retain it beyond the producer lambda's execution.
 *
 * @param <T> the element type emitted into the pipeline
 * @see io.heddle.Heddle#from(java.util.function.Consumer)
 * @see StageContext
 */
@FunctionalInterface
public interface Emitter<T> {

    /**
     * Delivers one item to the downstream pipeline stage.
     *
     * <p>This method may block if the downstream channel is at capacity. The calling
     * virtual thread parks until a slot becomes available, releasing its carrier
     * thread without blocking a platform thread. Calling this method after the
     * pipeline has been cancelled or stopped has no observable effect.
     *
     * @param item the item to deliver downstream
     */
    void emit(T item);
}
