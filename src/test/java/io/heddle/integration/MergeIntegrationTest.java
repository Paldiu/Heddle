package io.heddle.integration;

import io.heddle.Heddle;
import io.heddle.config.MergeStrategy;
import io.heddle.config.PipelineOptions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class MergeIntegrationTest {

    @Test
    void mergeIterablesProducesAllItems() {
        List<Integer> result = Heddle.merge(List.of(1, 2), List.of(3, 4), List.of(5))
                .toList();
        assertThat(result).containsExactlyInAnyOrder(1, 2, 3, 4, 5);
    }

    @Test
    void mergeSingleIterableIsIdentity() {
        List<Integer> result = Heddle.merge(List.of(1, 2, 3)).toList();
        assertThat(result).containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    void mergeEmptyIterablesProducesNothing() {
        List<Integer> result = Heddle.merge(List.<Integer>of(), List.<Integer>of()).toList();
        assertThat(result).isEmpty();
    }

    @Test
    void mergePipelinesProducesAllItems() {
        List<Integer> result = Heddle.merge(
                Heddle.range(0, 3),
                Heddle.range(10, 13)
        ).toList();
        assertThat(result).hasSize(6);
        assertThat(result).containsExactlyInAnyOrder(0, 1, 2, 10, 11, 12);
    }

    @Test
    void mergeEmptyPipelinesProducesNothing() {
        List<Integer> result = Heddle.merge(
                Heddle.<Integer>empty(),
                Heddle.<Integer>empty()
        ).toList();
        assertThat(result).isEmpty();
    }

    @Test
    void mergeSinglePipelineIsIdentity() {
        List<Integer> result = Heddle.merge(Heddle.range(0, 5)).toList();
        assertThat(result).containsExactlyInAnyOrder(0, 1, 2, 3, 4);
    }

    @Test
    void mergePipelinesFailFastOnUpstreamError() {
        assertThatThrownBy(() ->
                Heddle.merge(
                        Heddle.range(0, 5),
                        Heddle.of(99).map(n -> { throw new RuntimeException("upstream-fail"); })
                ).toList()
        ).isInstanceOf(RuntimeException.class);
    }

    @Test
    void mergePipelinesBestEffortCollectsAllErrors() {
        assertThatThrownBy(() ->
                Heddle.merge(
                        Heddle.of(1).map(n -> { throw new RuntimeException("err1"); }),
                        Heddle.of(2).map(n -> { throw new RuntimeException("err2"); })
                ).withOptions(PipelineOptions.defaults().withMergeStrategy(MergeStrategy.BEST_EFFORT))
                 .toList()
        ).isInstanceOf(RuntimeException.class)
         .hasMessageContaining("2 error(s)");
    }
}
