package io.heddle.integration;

import io.heddle.Heddle;
import io.heddle.Pipeline;
import io.heddle.api.PipelineHandle;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.*;

class TeeIntegrationTest {

    @Test
    void teeConsumerReceivesAllItems() {
        List<Integer> branch = Collections.synchronizedList(new ArrayList<>());
        List<Integer> result = Heddle.range(0, 5)
                .tee((Consumer<Integer>) branch::add)
                .toList();
        assertThat(result).containsExactly(0, 1, 2, 3, 4);
        assertThat(branch).containsExactlyInAnyOrder(0, 1, 2, 3, 4);
    }

    @Test
    void teeDoesNotMutateMainPipeline() {
        List<String> result = Heddle.of("a", "b", "c")
                .tee((Consumer<String>) item -> { /* side-effect only */ })
                .map(String::toUpperCase)
                .toList();
        assertThat(result).containsExactly("A", "B", "C");
    }

    @Test
    void teePipelineBranchReceivesAllItems() {
        List<Integer> branchCollected = Collections.synchronizedList(new ArrayList<>());
        List<Integer> result = Heddle.range(0, 5)
                .tee((Function<Pipeline<Integer>, PipelineHandle>)
                        p -> p.sink((Consumer<Integer>) branchCollected::add))
                .toList();
        assertThat(result).containsExactly(0, 1, 2, 3, 4);
        assertThat(branchCollected).containsExactlyInAnyOrder(0, 1, 2, 3, 4);
    }

    @Test
    void teeBranchErrorDoesNotFailMainPipeline() {
        List<Integer> result = Heddle.range(0, 5)
                .tee((Consumer<Integer>) item -> { throw new RuntimeException("branch-error"); })
                .toList();
        assertThat(result).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    void teeEmptySource() {
        List<Integer> branch = Collections.synchronizedList(new ArrayList<>());
        List<Integer> result = Heddle.<Integer>empty()
                .tee((Consumer<Integer>) branch::add)
                .toList();
        assertThat(result).isEmpty();
        assertThat(branch).isEmpty();
    }
}
