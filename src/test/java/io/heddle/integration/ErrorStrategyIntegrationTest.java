package io.heddle.integration;

import io.heddle.Heddle;
import io.heddle.api.PipelineHandle;
import io.heddle.error.ErrorStrategy;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.*;

class ErrorStrategyIntegrationTest {

    @Test
    void defaultStopStrategyFailsPipeline() {
        PipelineHandle handle = Heddle.range(0, 10)
                .map(n -> {
                    if (n == 5) throw new RuntimeException("stop-test");
                    return n;
                })
                .sink((Consumer<Integer>) item -> {});
        handle.start();
        handle.awaitCompletion();
        assertThat(handle.isFailed()).isTrue();
    }

    @Test
    void skipStrategyDropsBadItemsAndContinues() {
        List<Integer> result = Heddle.range(0, 6)
                .map(n -> {
                    if (n == 3) throw new RuntimeException("skip-me");
                    return n;
                })
                .onError(new ErrorStrategy.Skip())
                .toList();
        assertThat(result).containsExactly(0, 1, 2, 4, 5);
    }

    @Test
    void skipStrategyOnMultipleBadItems() {
        List<Integer> result = Heddle.range(0, 5)
                .map(n -> {
                    if (n % 2 == 0) throw new RuntimeException("even-bad");
                    return n;
                })
                .onError(new ErrorStrategy.Skip())
                .toList();
        assertThat(result).containsExactly(1, 3);
    }

    @Test
    void retrySucceedsWithinLimit() {
        List<Integer> processed = Collections.synchronizedList(new ArrayList<>());
        List<Integer> result = Heddle.of(1)
                .map(n -> {
                    processed.add(n);
                    if (processed.size() < 2) throw new RuntimeException("retry-me");
                    return n * 10;
                })
                .onError(new ErrorStrategy.Retry(3))
                .toList();
        assertThat(result).containsExactly(10);
        assertThat(processed).hasSize(2);
    }

    @Test
    void retryExhaustedFailsPipeline() {
        PipelineHandle handle = Heddle.of(42)
                .<Integer>map(n -> { throw new RuntimeException("always-fail"); })
                .onError(new ErrorStrategy.Retry(2))
                .sink((Consumer<Integer>) item -> {});
        handle.start();
        handle.awaitCompletion();
        assertThat(handle.isFailed()).isTrue();
    }

    @Test
    void deadLetterRoutesBadItemsToSink() {
        List<Object> dlq = Collections.synchronizedList(new ArrayList<>());
        List<Integer> results = Heddle.range(0, 5)
                .map(n -> {
                    if (n == 2) throw new RuntimeException("dl-item");
                    return n;
                })
                .onError(ErrorStrategy.deadLetterTo((item, cause) -> dlq.add(item)))
                .toList();
        assertThat(results).containsExactly(0, 1, 3, 4);
        assertThat(dlq).containsExactly(2);
    }

    @Test
    void deadLetterPreservesCause() {
        List<Throwable> causes = Collections.synchronizedList(new ArrayList<>());
        RuntimeException expected = new RuntimeException("specific-cause");
        Heddle.of(99)
                .map(n -> { throw expected; })
                .onError(ErrorStrategy.deadLetterTo((item, cause) -> causes.add(cause)))
                .toList();
        assertThat(causes).hasSize(1);
        assertThat(causes.get(0)).isSameAs(expected);
    }

    @Test
    void skipAllItemsFails() {
        List<Integer> result = Heddle.range(0, 5)
                .<Integer>map(n -> { throw new RuntimeException("all-skip"); })
                .onError(new ErrorStrategy.Skip())
                .toList();
        assertThat(result).isEmpty();
    }
}
