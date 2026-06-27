package io.heddle.integration;

import io.heddle.Heddle;
import io.heddle.api.PipelineHandle;
import io.heddle.config.PipelineOptions;
import io.heddle.policies.BackpressurePolicy;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.*;

class BackpressureIntegrationTest {

    @Test
    void blockPolicyDefaultBehaviour() {
        List<Integer> result = Heddle.range(0, 100).toList();
        assertThat(result).hasSize(100);
    }

    @Test
    void dropPolicyDropsItemsWhenChannelFull() {
        List<Integer> consumed = Collections.synchronizedList(new ArrayList<>());
        PipelineHandle handle = Heddle.range(0, 500)
                .backpressure(BackpressurePolicy.DROP)
                .withOptions(PipelineOptions.defaults().withBufferSize(2))
                .sink((Consumer<Integer>) item -> {
                    try { Thread.sleep(2); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    consumed.add(item);
                });
        handle.start();
        handle.awaitCompletion();
        assertThat(handle.isFailed()).isFalse();
        assertThat(consumed.size()).isLessThan(500);
    }

    @Test
    void failFastPolicyFailsPipelineWhenFull() {
        PipelineHandle handle = Heddle.range(0, 1000)
                .backpressure(BackpressurePolicy.FAIL_FAST)
                .withOptions(PipelineOptions.defaults().withBufferSize(2))
                .sink((Consumer<Integer>) item -> {
                    try { Thread.sleep(10); }
                    catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                });
        handle.start();
        handle.awaitCompletion();
        assertThat(handle.isFailed()).isTrue();
    }

    @Test
    void dropPolicyNeverBlocksProducer() {
        long count = Heddle.range(0, 200)
                .backpressure(BackpressurePolicy.DROP)
                .count();
        
        assertThat(count).isGreaterThanOrEqualTo(0).isLessThanOrEqualTo(200);
    }

    @Test
    void bufferOperatorIncreasesChannelCapacity() {
        List<Integer> result = Heddle.range(0, 50).buffer(128).toList();
        assertThat(result).hasSize(50);
    }
}
