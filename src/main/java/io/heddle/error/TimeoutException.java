package io.heddle.error;

/**
 * Thrown when a per-item processing timeout or a pipeline-level deadline elapses.
 *
 * <p>Per-item timeouts are raised by {@link io.heddle.Pipeline#timeout(java.time.Duration)}
 * when a single stage takes longer than the configured limit. These are recoverable:
 * the exception routes through the stage's {@link ErrorStrategy}, and the pipeline
 * may continue processing subsequent items.
 *
 * <p>Pipeline-level deadlines, configured via
 * {@link io.heddle.config.PipelineOptions#withDeadline(java.time.Duration)}, signal a
 * hard failure that terminates the entire pipeline.
 *
 * @see io.heddle.Pipeline#timeout(java.time.Duration)
 * @see io.heddle.config.PipelineOptions#withDeadline(java.time.Duration)
 */
public final class TimeoutException extends HeddleException {

    private final long deadlineNanos;

    /**
     * Constructs a {@code TimeoutException} with the given detail message and deadline.
     *
     * @param message       the detail message
     * @param deadlineNanos the deadline that elapsed, expressed as a nanosecond value
     *                      consistent with {@link System#nanoTime()}
     */
    public TimeoutException(String message, long deadlineNanos) {
        super(message);
        this.deadlineNanos = deadlineNanos;
    }

    /**
     * Constructs a {@code TimeoutException} with the given detail message, deadline,
     * and underlying cause.
     *
     * @param message       the detail message
     * @param deadlineNanos the deadline that elapsed, expressed as a nanosecond value
     *                      consistent with {@link System#nanoTime()}
     * @param cause         the exception that triggered this failure
     */
    public TimeoutException(String message, long deadlineNanos, Throwable cause) {
        super(message, cause);
        this.deadlineNanos = deadlineNanos;
    }

    /**
     * Returns the deadline that elapsed, expressed as a nanosecond timestamp consistent
     * with {@link System#nanoTime()}.
     *
     * @return the deadline in nanoseconds
     */
    public long deadlineNanos() { return deadlineNanos; }
}
