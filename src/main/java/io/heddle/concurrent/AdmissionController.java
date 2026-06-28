package io.heddle.concurrent;

import io.heddle.error.HeddleException;
import io.heddle.memory.MemoryGuard;

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
 *   <li>{@code AdmissionController} (Semaphore) - outermost; acquired per item.</li>
 *   <li>{@link ChildThreadTracker} (ReentrantLock) - acquired to track/untrack children.</li>
 *   <li>{@link io.heddle.context.PipelineContext} - lock-free (AtomicReference only).</li>
 * </ol>
 * Code that holds an AdmissionController permit MAY acquire the ChildThreadTracker
 * lock. The reverse (holding ChildThreadTracker while acquiring a permit) is
 * forbidden and will deadlock.
 */
public final class AdmissionController {

    public static final long DEFAULT_ACQUIRE_TIMEOUT_MS = 30_000;

    private final Semaphore semaphore;
    private final long acquireTimeoutMs;
    private final MemoryGuard memoryGuard;

    public AdmissionController(int maxConcurrent) {
        this(maxConcurrent, DEFAULT_ACQUIRE_TIMEOUT_MS, null);
    }

    public AdmissionController(int maxConcurrent, long acquireTimeoutMs) {
        this(maxConcurrent, acquireTimeoutMs, null);
    }

    /**
     * Creates a controller with a concurrency cap and an optional memory guard.
     *
     * <p>When {@code memoryGuard} is non-null, {@link #acquire()} first waits for
     * heap headroom to recover before competing for a semaphore permit. This prevents
     * new child virtual threads from being spawned while the heap is under pressure,
     * stopping allocation stalls before the JVM hits its memory ceiling.
     *
     * @param maxConcurrent    maximum in-flight items; must be positive
     * @param acquireTimeoutMs semaphore acquire timeout in milliseconds; must be positive
     * @param memoryGuard      optional heap-headroom gate; {@code null} disables the check
     */
    public AdmissionController(int maxConcurrent, long acquireTimeoutMs, MemoryGuard memoryGuard) {
        if (maxConcurrent <= 0)    throw new IllegalArgumentException("maxConcurrent must be positive");
        if (acquireTimeoutMs <= 0) throw new IllegalArgumentException("acquireTimeoutMs must be positive");
        this.semaphore        = new Semaphore(maxConcurrent);
        this.acquireTimeoutMs = acquireTimeoutMs;
        this.memoryGuard      = memoryGuard;
    }

    /**
     * Acquire one permit, blocking up to the configured timeout.
     *
     * <p>If a {@link MemoryGuard} was supplied at construction, this method first
     * waits until heap headroom is above the guard's threshold before competing for
     * the semaphore permit. The calling virtual thread parks while waiting, releasing
     * its carrier thread so the JVM and GC can continue unimpeded.
     *
     * @throws HeddleException if the semaphore timeout expires (possible deadlock),
     *         or the thread is interrupted while waiting for headroom or the semaphore
     */
    public void acquire() {
        try {
            if (memoryGuard != null) {
                memoryGuard.awaitHeadroom();
            }
            if (!semaphore.tryAcquire(acquireTimeoutMs, TimeUnit.MILLISECONDS)) {
                throw new HeddleException(
                        "AdmissionController timed out after " + acquireTimeoutMs +
                        "ms; stage may be deadlocked or excessively backpressured");
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
