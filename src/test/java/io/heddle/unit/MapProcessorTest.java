package io.heddle.unit;

import io.heddle.Heddle;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class MapProcessorTest {

    @Test
    void transformsAllItems() {
        List<String> result = Heddle.range(1, 4).map(n -> "item-" + n).toList();
        assertThat(result).containsExactly("item-1", "item-2", "item-3");
    }

    @Test
    void emptySourceProducesEmptyOutput() {
        List<Integer> result = Heddle.<String>empty().map(String::length).toList();
        assertThat(result).isEmpty();
    }

    @Test
    void typeChangeMapping() {
        List<Integer> result = Heddle.of("a", "bb", "ccc").map(String::length).toList();
        assertThat(result).containsExactly(1, 2, 3);
    }

    @Test
    void chainedMaps() {
        List<Integer> result = Heddle.range(1, 5).map(n -> n * 2).map(n -> n + 1).toList();
        assertThat(result).containsExactly(3, 5, 7, 9);
    }

    @Test
    void identityMap() {
        List<Integer> result = Heddle.range(0, 5).map(n -> n).toList();
        assertThat(result).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    void mapPreservesCount() {
        long count = Heddle.range(0, 100).map(n -> n * n).count();
        assertThat(count).isEqualTo(100);
    }

    @Test
    void mapToBoxedType() {
        List<Long> result = Heddle.range(0, 3).map(n -> (long) n).toList();
        assertThat(result).containsExactly(0L, 1L, 2L);
    }

    @Test
    void mapAsync() {
        List<String> result = Heddle.range(0, 4)
                .mapAsync(n -> "x" + n, 2)
                .toList();
        assertThat(result).hasSize(4);
        assertThat(result).containsExactlyInAnyOrder("x0", "x1", "x2", "x3");
    }
}
