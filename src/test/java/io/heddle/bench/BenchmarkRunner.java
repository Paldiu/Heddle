package io.heddle.bench;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Runs all Heddle JMH benchmarks. Execute via:
 * <pre>
 *   mvn test-compile exec:java \
 *       -Dexec.mainClass=io.heddle.bench.BenchmarkRunner \
 *       -Dexec.classpathScope=test
 * </pre>
 * Benchmarks are excluded from {@code mvn test} by surefire configuration.
 *
 * <p>Profilers enabled:
 * <ul>
 *   <li>{@code gc} - reports normalized allocation rate (bytes/op); validates back pressure
 *       under generational ZGC.</li>
 *   <li>{@code perfnorm} - reports hardware counters such as {@code L1-dcache-load-misses}
 *       and {@code cycles} per operation. Requires Linux {@code perf}; remove this profiler
 *       when running on Windows or macOS.</li>
 * </ul>
 *
 * <p>Thread discipline: {@code threads(1)} keeps a single JMH platform thread so the
 * virtual-thread scheduler inside Heddle drives all concurrency. Never set {@code @Threads(n)}
 * on individual benchmark classes - that spins up extra platform threads and corrupts VT
 * carrier-pool measurements.
 */
public class BenchmarkRunner {

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include("io\\.heddle\\.bench\\..*")
                .threads(1)
                .addProfiler("gc")
                .addProfiler("perfnorm")
                .build();
        new Runner(opt).run();
    }
}
