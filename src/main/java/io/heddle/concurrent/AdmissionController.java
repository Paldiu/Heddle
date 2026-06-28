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
 * <p><b>Striped semaphores.</b> A single {@code Semaphore(N)} serialises all
 * concurrent acquire/release operations on one AQS {@code state} field, causing
 * memory-bus contention when many virtual threads compete simultaneously. This
 * implementation uses {@code N} independent {@code Semaphore(1)} instances so
 * that each acquire/release CAS operates on a distinct cache line.
 *
 * <p>{@link #acquire()} returns the index of the acquired stripe; callers must
 * pass that index back to {@link #release(int)} to preserve permit symmetry.
 *
 * <p><b>Lock hierarchy.</b> The lock order in Heddle is:
 * <ol>
 *   <li>{@code AdmissionController} (Semaphore stripes) - outermost; acquired per item.</li>
 *   <li>{@link ChildThreadTracker} (ReentrantLock) - acquired to track/untrack children.</li>
 *   <li>{@link io.heddle.context.PipelineContext} - lock-free (AtomicReference only).</li>
 * </ol>
 * Code that holds an AdmissionController permit MAY acquire the ChildThreadTracker
 * lock. The reverse (holding ChildThreadTracker while acquiring a permit) is
 * forbidden and will deadlock.
 */
public final class AdmissionController {

    public static final long DEFAULT_ACQUIRE_TIMEOUT_MS = 30_000;

    private static final long SLOW_PATH_SLICE_NS = 1_000_000L; // 1 ms per stripe poll

    private final Semaphore[] stripes;
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
        this.stripes          = new Semaphore[maxConcurrent];
        for (int i = 0; i < maxConcurrent; i++) this.stripes[i] = new Semaphore(1);
        this.acquireTimeoutMs = acquireTimeoutMs;
        this.memoryGuard      = memoryGuard;
    }

    /**
     * Acquires one permit from any available stripe, blocking up to the configured timeout.
     *
     * <p>Fast path: one non-blocking scan of all stripes starting from the calling
     * thread's preferred stripe (derived from thread ID). If a free stripe is found
     * it is claimed immediately with no blocking.
     *
     * <p>Slow path: all stripes are occupied. The method rotates through stripes with
     * 1 ms {@code tryAcquire} slices; virtual threads unmount their carrier during
     * each park, so no platform thread is consumed while waiting.
     *
     * <p>If a {@link MemoryGuard} was supplied at construction, this method first
     * waits until heap headroom is above the guard's threshold before competing for
     * a stripe.
     *
     * @return the index of the acquired stripe; must be passed to {@link #release(int)}
     * @throws HeddleException if the timeout expires or the thread is interrupted
     */
    public int acquire() {
        try {
            if (memoryGuard != null) {
                memoryGuard.awaitHeadroom();
            }

            int base = stripeBase();

            // Fast path: one non-blocking scan
            for (int i = 0; i < stripes.length; i++) {
                int idx = (base + i) % stripes.length;
                if (stripes[idx].tryAcquire()) return idx;
            }

            // Slow path: rotate with 1 ms slices until deadline
            long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(acquireTimeoutMs);
            while (true) {
                for (int i = 0; i < stripes.length; i++) {
                    int idx = (base + i) % stripes.length;
                    long remainingNs = deadlineNs - System.nanoTime();
                    if (remainingNs <= 0) throw timeout();
                    long sliceNs = Math.min(remainingNs, SLOW_PATH_SLICE_NS);
                    if (stripes[idx].tryAcquire(sliceNs, TimeUnit.NANOSECONDS)) return idx;
                }
                if (System.nanoTime() >= deadlineNs) throw timeout();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HeddleException("AdmissionController acquire interrupted", e);
        }
    }

    /**
     * Releases the permit held on the given stripe.
     *
     * @param stripe the value returned by the matching {@link #acquire()} call
     */
    public void release(int stripe) {
        stripes[stripe].release();
    }

    public int availablePermits() {
        int sum = 0;
        for (Semaphore s : stripes) sum += s.availablePermits();
        return sum;
    }

    private int stripeBase() {
        int base = (int)(Thread.currentThread().threadId() % stripes.length);
        return base < 0 ? base + stripes.length : base;
    }

    private HeddleException timeout() {
        return new HeddleException(
                "AdmissionController timed out after " + acquireTimeoutMs +
                "ms; stage may be deadlocked or excessively backpressured");
    }
}
