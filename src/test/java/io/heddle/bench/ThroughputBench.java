package io.heddle.bench;

import io.heddle.Heddle;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Baseline throughput benchmarks. Run via {@link BenchmarkRunner} or:
 * <pre>
 *   mvn test-compile exec:java -Dexec.mainClass=io.heddle.bench.BenchmarkRunner \
 *       -Dexec.classpathScope=test
 * </pre>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class ThroughputBench {

    @Param({"1000", "100000"})
    int itemCount;

    @Benchmark
    public long mapFilterCount() {
        return Heddle.range(0, itemCount)
                .map(n -> n * 2)
                .filter(n -> n % 4 == 0)
                .count();
    }

    @Benchmark
    public List<Integer> toList() {
        return Heddle.range(0, itemCount).toList();
    }

    @Benchmark
    public List<List<Integer>> batchOf100() {
        return Heddle.range(0, itemCount).batch(100).toList();
    }

    @Benchmark
    public long distinct() {
        return Heddle.range(0, itemCount).distinct().count();
    }

    @Benchmark
    public long flatMapSequential() {
        return Heddle.range(0, itemCount / 10)
                .flatMap(n -> List.of(n, n + 1, n + 2, n + 3, n + 4))
                .count();
    }

    @Benchmark
    public long flatMapConcurrent() {
        return Heddle.range(0, itemCount / 10)
                .flatMap(n -> List.of(n, n + 1, n + 2, n + 3, n + 4), 4)
                .count();
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(ThroughputBench.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}
