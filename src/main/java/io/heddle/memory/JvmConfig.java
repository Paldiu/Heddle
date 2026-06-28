package io.heddle.memory;

import io.heddle.util.Hardware;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * JVM startup configuration recommendations for compute-intensive pipelines.
 *
 * <p>The JVM's default GC and thread settings are tuned for general-purpose
 * workloads. Pipelines that spawn many short-lived worker VTs producing dense
 * temporary objects need a tighter configuration to avoid two failure modes:
 * <ol>
 *   <li><b>GC pause stalls</b>: a GC that does not separate short-lived and
 *       long-lived objects must scan the entire heap on every collection, causing
 *       multi-millisecond pauses that starve the pipeline scheduler.</li>
 *   <li><b>GC thread over-allocation</b>: giving the GC more concurrent threads
 *       than the workload justifies steals physical cores from VT math, defeating
 *       the purpose of the concurrency cap.</li>
 * </ol>
 *
 * <p>Detection uses {@link ProcessHandle#current()} which is available in
 * {@code java.base} without any additional module configuration.
 *
 * <p>Call {@link #suggestions()} at application startup and log or print the result
 * so operators can adjust launch scripts without having to hunt for documentation.
 */
public final class JvmConfig {

    private JvmConfig() {}

    /**
     * Returns {@code true} if the JVM command line contains {@code -XX:+UseZGC}.
     *
     * <p>Detection relies on {@link ProcessHandle.Info#arguments()} which may return
     * an empty optional on some operating systems or when the process was started
     * without the necessary permissions. In that case this method returns
     * {@code false} conservatively.
     */
    public static boolean isZgcActive() {
        return jvmArgs().stream().anyMatch(arg -> arg.equals("-XX:+UseZGC"));
    }

    /**
     * Returns {@code true} if the JVM command line contains both {@code -XX:+UseZGC}
     * and {@code -XX:+ZGenerational}.
     *
     * <p>Generational ZGC separates the heap into young and old generations.
     * Short-lived objects: temporaries, intermediate buffers, coordinate wrappers —
     * are collected in the young generation in microseconds without touching
     * long-lived data. This eliminates the allocation pressure that high-throughput
     * pipelines generate without penalising the main application heap.
     */
    public static boolean isGenerationalZgcActive() {
        List<String> args = jvmArgs();
        return args.stream().anyMatch(arg -> arg.equals("-XX:+UseZGC"))
            && args.stream().anyMatch(arg -> arg.equals("-XX:+ZGenerational"));
    }

    /**
     * Returns a list of human-readable suggestions for JVM startup flags that would
     * improve pipeline throughput and GC behaviour.
     *
     * <p>The list is empty if the JVM is already well-configured. When non-empty,
     * log or print the entries at startup so operators can update the launch script.
     */
    public static List<String> suggestions() {
        List<String> result = new ArrayList<>();

        if (!isGenerationalZgcActive()) {
            result.add(
                "Generational ZGC is not active. " +
                "Add -XX:+UseZGC -XX:+ZGenerational to the JVM startup flags. " +
                "It separates short-lived objects (buffers, temporaries) from long-lived " +
                "data and collects them in the young generation in microseconds, " +
                "eliminating the allocation pressure that high-throughput pipelines generate.");
        }

        int recommended = Hardware.recommendedGcThreads();
        result.add(
            "Recommended concurrent GC thread count: " + recommended +
            " (-XX:ConcGCThreads=" + recommended + "). " +
            "Over-allocating GC threads steals physical cores from virtual-thread math. " +
            "The formula is max(1, physicalCores / 4).");

        return result;
    }

    /**
     * Returns the recommended JVM startup flags as a list of strings suitable for
     * appending to a launch script.
     */
    public static List<String> recommendedFlags() {
        return List.of(
            "-XX:+UseZGC",
            "-XX:+ZGenerational",
            "-XX:ConcGCThreads=" + Hardware.recommendedGcThreads()
        );
    }

    private static List<String> jvmArgs() {
        Optional<String[]> args = ProcessHandle.current().info().arguments();
        if (args.isEmpty()) return List.of();
        return List.of(args.get());
    }
}
