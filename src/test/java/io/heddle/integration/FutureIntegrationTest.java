package io.heddle.integration;

import io.heddle.Heddle;
import io.heddle.api.PipelineHandle;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.*;

class FutureIntegrationTest {

    @Test
    void fromFutureEmitsSingleItem() {
        CompletableFuture<String> future = CompletableFuture.completedFuture("hello");
        List<String> result = Heddle.from(future).toList();
        assertThat(result).containsExactly("hello");
    }

    @Test
    void fromFailedFutureFailsPipeline() {
        CompletableFuture<String> future = CompletableFuture.failedFuture(
                new RuntimeException("future-fail"));
        PipelineHandle handle = Heddle.from(future).sink((Consumer<String>) item -> {});
        handle.start();
        handle.awaitCompletion();
        assertThat(handle.isFailed()).isTrue();
    }

    @Test
    void awaitFuturesUnwrapsCollection() {
        List<CompletableFuture<Integer>> futures = List.of(
                CompletableFuture.completedFuture(1),
                CompletableFuture.completedFuture(2),
                CompletableFuture.completedFuture(3));
        List<Integer> result = Heddle.from(futures).<Integer>awaitFutures().toList();
        assertThat(result).containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    void awaitFuturesWithAsyncCompletion() throws Exception {
        CompletableFuture<Integer> slow = CompletableFuture.supplyAsync(() -> {
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return 42;
        });
        List<Integer> result = Heddle.from(List.of(slow)).<Integer>awaitFutures().toList();
        assertThat(result).containsExactly(42);
    }

    @Test
    void toFutureCompletesNormally() throws Exception {
        CompletableFuture<List<Integer>> future = Heddle.range(1, 4).map(n -> n * n).toFuture();
        List<Integer> result = future.get();
        assertThat(result).containsExactly(1, 4, 9);
    }

    @Test
    void toFutureCompletesExceptionallyOnFailure() {
        CompletableFuture<List<Object>> future = Heddle.of("x")
                .map(s -> { throw new RuntimeException("future-err"); })
                .toFuture();
        assertThatThrownBy(future::get)
                .hasCauseInstanceOf(RuntimeException.class)
                .cause()
                .hasMessage("future-err");
    }

    @Test
    void fromNullFutureThrows() {
        assertThatThrownBy(() -> Heddle.from((CompletableFuture<Object>) null))
                .isInstanceOf(NullPointerException.class);
    }
}
