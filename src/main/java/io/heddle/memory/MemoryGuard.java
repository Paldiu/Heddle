package io.heddle.memory;

/**
 * Monitors JVM heap headroom and gates virtual-thread spawning to prevent
 * allocation stalls before the JVM hits its memory ceiling.
 *
 * <p>Virtual threads are cheap to create, so a fast producer stage can
 * legitimately queue thousands of items before the pipeline drains them. Each
 * queued item spawns a child VT that allocates working objects. If the heap
 * fills before those objects are reclaimed, the JVM stalls allocations across
 * every thread simultaneously; a worse outcome than the backpressure it was
 * avoiding. This guard interrupts that spiral by pausing item admission when
 * free heap drops below a safety threshold, giving the GC time to catch up
 * before any new work is started.
 *
 * <p>The guard is checked inside {@link io.heddle.concurrent.AdmissionController#acquire()}
 * so the check is automatic on every child-VT spawn with no changes needed
 * in user stage code.
 *
 * <p>While waiting for headroom the calling virtual thread sleeps, which parks
 * it and releases its carrier thread, the JVM and GC can continue unimpeded
 * on the freed physical cores.
 *
 * <p>Memory figures are read from {@link Runtime#getRuntime()}, which is in
 * {@code java.base} and requires no additional module configuration.
 * <ul>
 *   <li>{@link Runtime#maxMemory()} - the configured heap ceiling ({@code -Xmx})</li>
 *   <li>{@link Runtime#totalMemory()} - heap currently committed from the OS</li>
 *   <li>{@link Runtime#freeMemory()} - free space within committed heap</li>
 * </ul>
 * Headroom = (maxMemory - (totalMemory - freeMemory)) / maxMemory.
 */
public final class MemoryGuard {

    /** Default minimum fraction of max heap that must remain free before new work is admitted. */
    public static final double DEFAULT_FREE_THRESHOLD = 0.20;

    /** Default interval between headroom polls while waiting. */
    public static final long DEFAULT_POLL_MS = 50;

    private final double freeThreshold;
    private final long pollMs;

    /**
     * Creates a guard with a custom threshold and poll interval.
     *
     * @param freeThreshold the minimum fraction of max heap that must be free before
     *                      new work is admitted; must be in (0, 1) exclusive
     * @param pollMs        how often to re-check heap headroom while waiting;
     *                      must be positive
     */
    public MemoryGuard(double freeThreshold, long pollMs) {
        if (freeThreshold <= 0.0 || freeThreshold >= 1.0)
            throw new IllegalArgumentException(
                    "freeThreshold must be in (0, 1) exclusive, got " + freeThreshold);
        if (pollMs <= 0)
            throw new IllegalArgumentException("pollMs must be positive, got " + pollMs);
        this.freeThreshold = freeThreshold;
        this.pollMs        = pollMs;
    }

    /**
     * Creates a guard with the default 20% free-heap threshold and 50 ms poll interval.
     */
    public static MemoryGuard ofDefaults() {
        return new MemoryGuard(DEFAULT_FREE_THRESHOLD, DEFAULT_POLL_MS);
    }

    /**
     * Returns the current fraction of max heap that is free.
     *
     * <p>A value of {@code 0.20} means 20% of the configured max heap is not yet
     * allocated to live objects. If the JVM was started without {@code -Xmx}, max
     * is {@link Long#MAX_VALUE} and this method returns {@code 1.0} (no pressure).
     */
    public double headroomFraction() {
        Runtime rt  = Runtime.getRuntime();
        long max    = rt.maxMemory();
        if (max == Long.MAX_VALUE) return 1.0;
        long used   = rt.totalMemory() - rt.freeMemory();
        return (double) (max - used) / max;
    }

    /**
     * Returns {@code true} if free heap has fallen below the configured threshold.
     */
    public boolean isPressured() {
        return headroomFraction() < freeThreshold;
    }

    /**
     * Blocks the calling thread until free heap is at or above the threshold.
     *
     * <p>While waiting the thread sleeps in {@code pollMs} increments. Virtual
     * threads park on {@link Thread#sleep}, releasing their carrier thread so
     * that the JVM and concurrent GC threads can continue on the freed cores.
     *
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void awaitHeadroom() throws InterruptedException {
        while (isPressured()) {
            Thread.sleep(pollMs);
        }
    }

    /** Returns the minimum free-heap fraction required before admission. */
    public double freeThreshold() { return freeThreshold; }

    /** Returns the poll interval in milliseconds used by {@link #awaitHeadroom()}. */
    public long pollMs() { return pollMs; }
}
