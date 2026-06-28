package io.heddle.config;

import io.heddle.memory.MemoryGuard;

import java.time.Duration;

/**
 * Immutable tuning configuration for a pipeline.
 *
 * <p>Obtain a starting point via {@link #defaults()} and produce customized instances
 * with the {@code with*} copy-and-mutate methods. Because {@code PipelineOptions} is
 * a record, all instances are immutable and safe to share across threads.
 *
 * <pre>{@code
 * pipeline.withOptions(PipelineOptions.defaults()
 *         .withBufferSize(256)
 *         .withDeadline(Duration.ofMinutes(5))
 *         .withMergeStrategy(MergeStrategy.BEST_EFFORT));
 * }</pre>
 *
 * @param bufferSize    the capacity of each inter-stage channel; must be positive
 * @param threadName    the base name used when naming pipeline virtual threads;
 *                      must not be blank
 * @param deadline      an optional wall-clock time limit for the entire pipeline;
 *                      {@code null} means no deadline
 * @param mergeStrategy the error-propagation policy applied when one upstream in a
 *                      {@link io.heddle.Heddle#merge(io.heddle.Pipeline[])} call fails
 *                      while others are still running; must not be {@code null}
 * @param memoryGuard   an optional heap-headroom gate applied to concurrent stages;
 *                      {@code null} disables memory-pressure backpressure
 * @see io.heddle.Pipeline#withOptions(PipelineOptions)
 */
public record PipelineOptions(int bufferSize, String threadName, Duration deadline,
                              MergeStrategy mergeStrategy, MemoryGuard memoryGuard) {

    /** The default inter-stage channel capacity. */
    public static final int    DEFAULT_BUFFER_SIZE = 64;

    /** The default base name applied to pipeline virtual threads. */
    public static final String DEFAULT_THREAD_NAME = "heddle";

    /**
     * Validates the record components on canonical construction.
     *
     * @throws IllegalArgumentException if {@code bufferSize} is not positive or
     *                                  {@code threadName} is blank
     * @throws NullPointerException     if {@code mergeStrategy} is {@code null}
     */
    public PipelineOptions {
        if (bufferSize <= 0)
            throw new IllegalArgumentException("bufferSize must be positive");
        if (threadName == null || threadName.isBlank())
            throw new IllegalArgumentException("threadName must not be blank");
        if (mergeStrategy == null)
            throw new NullPointerException("mergeStrategy must not be null");
    }

    /**
     * Returns a {@code PipelineOptions} instance with all fields set to their defaults:
     * buffer size {@value #DEFAULT_BUFFER_SIZE}, thread name {@value #DEFAULT_THREAD_NAME},
     * no deadline, and {@link MergeStrategy#FAIL_FAST}.
     *
     * @return the default pipeline options
     */
    public static PipelineOptions defaults() {
        return new PipelineOptions(DEFAULT_BUFFER_SIZE, DEFAULT_THREAD_NAME, null, MergeStrategy.FAIL_FAST, null);
    }

    /**
     * Returns a copy of this options instance with the inter-stage channel capacity
     * set to the specified value.
     *
     * @param size the new buffer size; must be positive
     * @return a new {@code PipelineOptions} with the updated buffer size
     * @throws IllegalArgumentException if {@code size} is not positive
     */
    public PipelineOptions withBufferSize(int size)           { return new PipelineOptions(size, threadName, deadline, mergeStrategy, memoryGuard); }

    /**
     * Returns a copy of this options instance with the virtual-thread base name set to
     * the specified value.
     *
     * @param name the new thread base name; must not be blank
     * @return a new {@code PipelineOptions} with the updated thread name
     * @throws IllegalArgumentException if {@code name} is blank
     */
    public PipelineOptions withThreadName(String name)        { return new PipelineOptions(bufferSize, name, deadline, mergeStrategy, memoryGuard); }

    /**
     * Returns a copy of this options instance with the pipeline deadline set to the
     * specified duration.
     *
     * <p>If the pipeline does not complete within the deadline after
     * {@link io.heddle.api.PipelineHandle#start()} is called, it is terminated with a
     * {@link io.heddle.error.TimeoutException}.
     *
     * @param d the wall-clock deadline duration; pass {@code null} to clear any
     *          existing deadline
     * @return a new {@code PipelineOptions} with the updated deadline
     */
    public PipelineOptions withDeadline(Duration d)           { return new PipelineOptions(bufferSize, threadName, d, mergeStrategy, memoryGuard); }

    /**
     * Returns a copy of this options instance with the merge strategy set to the
     * specified value.
     *
     * @param s the new merge strategy; must not be {@code null}
     * @return a new {@code PipelineOptions} with the updated merge strategy
     * @throws NullPointerException if {@code s} is {@code null}
     */
    public PipelineOptions withMergeStrategy(MergeStrategy s) { return new PipelineOptions(bufferSize, threadName, deadline, s, memoryGuard); }

    /**
     * Returns a copy of this options instance with the heap-headroom gate set to the
     * specified guard.
     *
     * <p>When non-null, concurrent stages pause spawning new child virtual threads
     * whenever free heap drops below the guard's threshold, giving the GC time to
     * reclaim short-lived objects before new allocation pressure is added.
     * Pass {@code null} to disable the check (the default).
     *
     * @param guard the memory guard to apply; {@code null} disables the check
     * @return a new {@code PipelineOptions} with the updated memory guard
     */
    public PipelineOptions withMemoryGuard(MemoryGuard guard) { return new PipelineOptions(bufferSize, threadName, deadline, mergeStrategy, guard); }
}
