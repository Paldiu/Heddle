package io.heddle.unit;

import io.heddle.Heddle;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class BatchProcessorTest {

    @Test
    void evenBatch() {
        List<List<Integer>> batches = Heddle.range(0, 6).batch(2).toList();
        assertThat(batches).hasSize(3);
        assertThat(batches.get(0)).containsExactly(0, 1);
        assertThat(batches.get(1)).containsExactly(2, 3);
        assertThat(batches.get(2)).containsExactly(4, 5);
    }

    @Test
    void partialBatchFlushedOnCompletion() {
        List<List<Integer>> batches = Heddle.range(0, 5).batch(3).toList();
        assertThat(batches).hasSize(2);
        assertThat(batches.get(0)).containsExactly(0, 1, 2);
        assertThat(batches.get(1)).containsExactly(3, 4);
    }

    @Test
    void singleItemFormsOnePartialBatch() {
        List<List<String>> batches = Heddle.of("only").batch(10).toList();
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0)).containsExactly("only");
    }

    @Test
    void emptySourceProducesNoBatches() {
        List<List<Integer>> batches = Heddle.<Integer>empty().batch(5).toList();
        assertThat(batches).isEmpty();
    }

    @Test
    void batchSizeExactlyMatchesItemCount() {
        List<List<Integer>> batches = Heddle.range(0, 4).batch(4).toList();
        assertThat(batches).hasSize(1);
        assertThat(batches.get(0)).containsExactly(0, 1, 2, 3);
    }

    @Test
    void zeroBatchSizeThrows() {
        assertThatThrownBy(() -> Heddle.range(0, 3).batch(0).toList())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void negativeBatchSizeThrows() {
        assertThatThrownBy(() -> Heddle.range(0, 3).batch(-1).toList())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void batchPreservesOrder() {
        List<List<Integer>> batches = Heddle.range(0, 9).batch(3).toList();
        assertThat(batches).hasSize(3);
        for (int i = 0; i < 3; i++) {
            int base = i * 3;
            assertThat(batches.get(i)).containsExactly(base, base + 1, base + 2);
        }
    }

    @Test
    void mapAfterBatch() {
        List<Integer> batchSizes = Heddle.range(0, 7)
                .batch(3)
                .map(List::size)
                .toList();
        assertThat(batchSizes).containsExactly(3, 3, 1);
    }
}
