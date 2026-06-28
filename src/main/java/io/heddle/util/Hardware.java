package io.heddle.util;

/**
 * Hardware introspection constants for the local JVM process.
 *
 * <p>Use these values to bound concurrency and size buffers to physical reality
 * rather than arbitrary constants. Virtual threads are logical constructs; when
 * a core context-switches between carrier threads it evicts its L1/L2 caches.
 * CPU-bound stages that oversubscribe physical cores therefore lose cache locality
 * and see a measurable throughput drop.
 *
 * <p>All values are captured once at class-load time. {@code availableProcessors()}
 * can theoretically change at runtime in containerized environments, but querying it
 * per-item would be expensive and the variation is rare enough that a snapshot is the
 * correct trade-off for a pipeline runtime.
 */
public final class Hardware {

    private Hardware() {}

    private static final int PHYSICAL_CORES = Runtime.getRuntime().availableProcessors();

    /**
     * Number of logical processors available to the JVM.
     *
     * <p>On a non-hyperthreaded machine this equals the physical core count. On a
     * hyperthreaded machine it equals logical threads (2&times; cores). Either way it
     * represents the maximum number of tasks that can run simultaneously without paying
     * a context-switch penalty, and therefore the correct upper bound for CPU-bound
     * concurrency.
     *
     * <p>For I/O-bound stages (HTTP calls, database writes) the VT scheduler parks the
     * carrier on blocking calls, so concurrency far above this value is fine. For
     * compute-bound stages (math, compression, serialisation) stay at or below this value.
     */
    public static int physicalCores() {
        return PHYSICAL_CORES;
    }

    /**
     * Cache-line size in bytes for the current architecture.
     *
     * <p>64 bytes on x86-64 and modern ARM. Use this constant to pad shared mutable
     * fields so that logically independent values do not land on the same cache line
     * and cause false-sharing between cores.
     */
    public static int cacheLineBytes() {
        return 64;
    }

    /**
     * Recommended number of concurrent GC threads for a compute-intensive workload.
     *
     * <p>Returns {@code max(1, physicalCores() / 4)}. The JVM default is roughly the
     * same formula but errs high on large machines. Over-allocating GC threads steals
     * physical cores from virtual-thread math: a GC thread occupying a core is a core
     * that cannot execute a VT carrier. Pass the result to {@code -XX:ConcGCThreads=N}
     * in the JVM startup flags to enforce this cap.
     */
    public static int recommendedGcThreads() {
        return Math.max(1, PHYSICAL_CORES / 4);
    }
}
