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
 */
public class BenchmarkRunner {

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include("io\\.heddle\\.bench\\..*")
                .build();
        new Runner(opt).run();
    }
}
