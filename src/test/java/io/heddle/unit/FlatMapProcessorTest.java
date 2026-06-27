package io.heddle.unit;

import io.heddle.Heddle;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class FlatMapProcessorTest {

    @Test
    void expandsEachItem() {
        List<Integer> result = Heddle.of(1, 2, 3)
                .flatMap(n -> List.of(n, n * 10))
                .toList();
        assertThat(result).containsExactly(1, 10, 2, 20, 3, 30);
    }

    @Test
    void emptyExpansionFiltersItem() {
        List<Integer> result = Heddle.range(0, 4)
                .flatMap(n -> n % 2 == 0 ? List.of(n) : List.<Integer>of())
                .toList();
        assertThat(result).containsExactly(0, 2);
    }

    @Test
    void splitByDelimiter() {
        List<String> result = Heddle.of("hello world", "foo bar")
                .flatMap(s -> List.of(s.split(" ")))
                .toList();
        assertThat(result).containsExactly("hello", "world", "foo", "bar");
    }

    @Test
    void emptySourceProducesEmptyOutput() {
        List<Integer> result = Heddle.<Integer>empty()
                .flatMap(n -> List.of(n, n + 1))
                .toList();
        assertThat(result).isEmpty();
    }

    @Test
    void singleExpansionPerItem() {
        List<Integer> result = Heddle.range(0, 5)
                .flatMap(n -> List.of(n))
                .toList();
        assertThat(result).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    void allEmptyExpansions() {
        List<Integer> result = Heddle.range(0, 5)
                .flatMap(n -> List.<Integer>of())
                .toList();
        assertThat(result).isEmpty();
    }

    @Test
    void concurrentFlatMapContainsAllItems() {
        List<Integer> result = Heddle.range(0, 5)
                .flatMap(n -> List.of(n, n + 100), 2)
                .toList();
        assertThat(result).hasSize(10);
        assertThat(result).contains(0, 1, 2, 3, 4, 100, 101, 102, 103, 104);
    }

    @Test
    void totalOutputCountMatchesExpansionProduct() {
        long count = Heddle.range(0, 10)
                .flatMap(n -> List.of(n, n, n))
                .count();
        assertThat(count).isEqualTo(30);
    }
}
