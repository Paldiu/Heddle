package io.heddle.unit;

import io.heddle.Heddle;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class DistinctProcessorTest {

    @Test
    void removesExactDuplicates() {
        List<Integer> result = Heddle.of(1, 2, 2, 3, 1, 4).distinct().toList();
        assertThat(result).containsExactlyInAnyOrder(1, 2, 3, 4);
        assertThat(result).hasSize(4);
    }

    @Test
    void preservesFirstOccurrenceOrder() {
        List<Integer> result = Heddle.of(3, 1, 2, 3, 1).distinct().toList();
        assertThat(result).containsExactly(3, 1, 2);
    }

    @Test
    void emptySourceStaysEmpty() {
        List<Integer> result = Heddle.<Integer>empty().distinct().toList();
        assertThat(result).isEmpty();
    }

    @Test
    void allUnique() {
        List<Integer> result = Heddle.range(0, 5).distinct().toList();
        assertThat(result).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    void allDuplicates() {
        List<String> result = Heddle.of("x", "x", "x", "x").distinct().toList();
        assertThat(result).containsExactly("x");
    }

    @Test
    void keyExtractorDeduplication() {
        List<String> result = Heddle.of("apple", "ant", "banana", "avocado")
                .distinct(s -> s.charAt(0))
                .toList();
        assertThat(result).containsExactly("apple", "banana");
    }

    @Test
    void lruEvictionAllowsReEmitAfterEviction() {
        
        List<Integer> result = Heddle.of(1, 2, 3, 1).distinct(2).toList();
        assertThat(result).containsExactly(1, 2, 3, 1);
    }

    @Test
    void failFastThrowsWhenCapExceeded() {
        assertThatThrownBy(() ->
                Heddle.of(1, 2, 3, 4).distinct(3, true).toList()
        ).isInstanceOf(RuntimeException.class);
    }

    @Test
    void failFastDoesNotThrowWithinCap() {
        List<Integer> result = Heddle.of(1, 2, 3, 1, 2).distinct(3, true).toList();
        assertThat(result).containsExactly(1, 2, 3);
    }

    @Test
    void distinctCombinedWithFilter() {
        List<Integer> result = Heddle.of(1, 1, 2, 2, 3, 3)
                .distinct()
                .filter(n -> n % 2 != 0)
                .toList();
        assertThat(result).containsExactly(1, 3);
    }
}
