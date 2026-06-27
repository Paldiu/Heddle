package io.heddle.concurrent;

import io.heddle.error.HeddleException;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Per-stage in-flight item cap. Bounds how many items a stage may be processing
 * simultaneously, independent of channel buffer capacity.
 *
 * <p>Unlike {@link io.heddle.channel.HeddleChannel}'s queue depth (which limits
 * items waiting to be picked up), the AdmissionController limits items actively
 * inside {@code Stage.process()}. This is useful for stages with expensive
 * side-effects (database writes, HTTP calls) where unbounded concurrency
 * causes resource exhaustion rather than back-pressure.
 *
 * <p><b>Lock hierarchy.</b> The lock order in Heddle is:
 * <ol>
 *   <li>{@code AdmissionController} (Semaphore) — outermost; acquired per item.</li>
 *   <li>{@link ChildThreadTracker} (ReentrantLock) — acquired to track/untrack children.</li>
 *   <li>{@link io.heddle.context.PipelineContext} — lock-free (AtomicReference only).</li>
 * </ol>
 * Code that holds an AdmissionController permit MAY acquire the ChildThreadTracker
 * lock. The reverse (holding ChildThreadTracker while acquiring a permit) is
 * forbidden and will deadlock.
 */
public final class AdmissionController {

    static final long DEFAULT_ACQUIRE_TIMEOUT_MS = 30_000;

    private final Semaphore semaphore;
    private final long acquireTimeoutMs;

    public AdmissionController(int maxConcurrent) {
        this(maxConcurrent, DEFAULT_ACQUIRE_TIMEOUT_MS);
    }

    public AdmissionController(int maxConcurrent, long acquireTimeoutMs) {
        if (maxConcurrent <= 0)    throw new IllegalArgumentException("maxConcurrent must be positive");
        if (acquireTimeoutMs <= 0) throw new IllegalArgumentException("acquireTimeoutMs must be positive");
        this.semaphore        = new Semaphore(maxConcurrent);
        this.acquireTimeoutMs = acquireTimeoutMs;
    }

    /**
     * Acquire one permit, blocking up to the configured timeout.
     *
     * @throws HeddleException if the timeout expires (possible deadlock) or the
     *         thread is interrupted
     */
    public void acquire() {
        try {
            if (!semaphore.tryAcquire(acquireTimeoutMs, TimeUnit.MILLISECONDS)) {
                throw new HeddleException(
                        "AdmissionController timed out after " + acquireTimeoutMs +
                        "ms — stage may be deadlocked or excessively backpressured");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HeddleException("AdmissionController acquire interrupted", e);
        }
    }

    public void release() {
        semaphore.release();
    }

    public int availablePermits() {
        return semaphore.availablePermits();
    }
}
