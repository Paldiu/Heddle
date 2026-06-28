package io.heddle.bench;

import io.heddle.Heddle;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Compares sequential vs concurrent flatMap expansion at varying fanout and concurrency.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 8, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {"-XX:+UseZGC", "-XX:+ZGenerational"})
public class FlatMapBench {

    @Param({"100", "1000"})
    int outerItems;

    @Param({"5", "20"})
    int fanout;

    @Benchmark
    public long sequential() {
        final int f = fanout;
        return Heddle.range(0, outerItems)
                .flatMap(n -> buildList(n, f))
                .count();
    }

    @Benchmark
    @OperationsPerInvocation(1)
    public long concurrent2() {
        final int f = fanout;
        return Heddle.range(0, outerItems)
                .flatMap(n -> buildList(n, f), 2)
                .count();
    }

    @Benchmark
    @OperationsPerInvocation(1)
    public long concurrent8() {
        final int f = fanout;
        return Heddle.range(0, outerItems)
                .flatMap(n -> buildList(n, f), 8)
                .count();
    }

    private static List<Integer> buildList(int base, int size) {
        List<Integer> out = new java.util.ArrayList<>(size);
        for (int i = 0; i < size; i++) out.add(base + i);
        return out;
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(FlatMapBench.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}
