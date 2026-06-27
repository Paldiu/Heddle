package io.heddle.integration;

import io.heddle.Heddle;
import io.heddle.api.PipelineHandle;
import io.heddle.config.PipelineOptions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.*;

class LifecycleTest {

    @Test
    void handleStartedOnlyOnce() {
        PipelineHandle handle = Heddle.range(0, 5).sink((Consumer<Integer>) item -> {});
        handle.start();
        assertThatThrownBy(handle::start).isInstanceOf(IllegalStateException.class);
        handle.awaitCompletion();
    }

    @Test
    void isCompleteAfterAwait() {
        PipelineHandle handle = Heddle.range(0, 10).sink((Consumer<Integer>) item -> {});
        handle.start();
        handle.awaitCompletion();
        assertThat(handle.isComplete()).isTrue();
        assertThat(handle.isRunning()).isFalse();
        assertThat(handle.isFailed()).isFalse();
        assertThat(handle.isCancelled()).isFalse();
    }

    @Test
    void onCompleteCallbackFires() {
        AtomicBoolean fired = new AtomicBoolean();
        PipelineHandle handle = Heddle.range(0, 5).sink((Consumer<Integer>) item -> {});
        handle.onComplete(() -> fired.set(true));
        handle.start();
        handle.awaitCompletion();
        assertThat(fired.get()).isTrue();
    }

    @Test
    void onCompleteDoesNotFireOnFailure() {
        AtomicBoolean fired = new AtomicBoolean();
        PipelineHandle handle = Heddle.of("x")
                .map(s -> { throw new RuntimeException("oops"); })
                .sink((Consumer<Object>) item -> {});
        handle.onComplete(() -> fired.set(true));
        handle.start();
        handle.awaitCompletion();
        assertThat(fired.get()).isFalse();
    }

    @Test
    void onErrorCallbackFiresOnFailure() {
        List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
        PipelineHandle handle = Heddle.of("x")
                .map(s -> { throw new RuntimeException("cb-test"); })
                .sink((Consumer<Object>) item -> {});
        handle.onError(errors::add);
        handle.start();
        handle.awaitCompletion();
        assertThat(errors).hasSize(1);
        assertThat(errors.get(0)).isInstanceOf(RuntimeException.class);
    }

    @Test
    void failureCauseContainsOriginatingException() {
        RuntimeException original = new RuntimeException("root-cause");
        PipelineHandle handle = Heddle.of("item")
                .map(s -> { throw original; })
                .sink((Consumer<Object>) item -> {});
        handle.start();
        handle.awaitCompletion();
        assertThat(handle.isFailed()).isTrue();
        assertThat(handle.failureCause()).isPresent();
        assertThat(handle.failureCause().orElseThrow()).isSameAs(original);
    }

    @Test
    void cancelStopsPipeline() {
        PipelineHandle handle = Heddle.tick(Duration.ofMillis(1)).sink((Consumer<Long>) item -> {});
        handle.start();
        Thread.ofVirtual().start(() -> {
            try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            handle.cancel();
        });
        handle.awaitCompletion();
        assertThat(handle.isCancelled()).isTrue();
        assertThat(handle.isFailed()).isFalse();
    }

    @Test
    void stopSignalsSourceToFinish() {
        List<Long> collected = Collections.synchronizedList(new ArrayList<>());
        PipelineHandle handle = Heddle.tick(Duration.ofMillis(5)).sink((Consumer<Long>) collected::add);
        handle.start();
        try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        handle.stop();
        handle.awaitCompletion();
        assertThat(handle.isFailed()).isFalse();
        assertThat(handle.isCancelled()).isFalse();
        assertThat(collected).isNotEmpty();
    }

    @Test
    void deadlineCancelsLongRunningPipeline() {
        PipelineHandle handle = Heddle.tick(Duration.ofMillis(1))
                .withOptions(PipelineOptions.defaults().withDeadline(Duration.ofMillis(100)))
                .sink((Consumer<Long>) item -> {});
        handle.start();
        handle.awaitCompletion();
        assertThat(handle.isCancelled()).isTrue();
    }

    @Test
    void awaitCompletionWithTimeoutThrowsWhenExceeded() {
        PipelineHandle handle = Heddle.tick(Duration.ofMillis(10)).sink((Consumer<Long>) item -> {});
        handle.start();
        assertThatThrownBy(() -> handle.awaitCompletion(Duration.ofMillis(50)))
                .isInstanceOf(java.util.concurrent.TimeoutException.class);
        handle.awaitCompletion();
    }

    @Test
    void statsAreAvailableAfterCompletion() {
        PipelineHandle handle = Heddle.range(0, 20).sink((Consumer<Integer>) item -> {});
        handle.start();
        handle.awaitCompletion();
        var stats = handle.stats();
        assertThat(stats).isNotNull();
        assertThat(stats.itemsProcessed()).isEqualTo(20);
        assertThat(stats.elapsed()).isGreaterThanOrEqualTo(Duration.ZERO);
    }
}
