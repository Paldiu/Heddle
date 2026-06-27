package io.heddle.unit;

import io.heddle.Heddle;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class FilterProcessorTest {

    @Test
    void keepMatchingItems() {
        List<Integer> result = Heddle.range(0, 10).filter(n -> n % 2 == 0).toList();
        assertThat(result).containsExactly(0, 2, 4, 6, 8);
    }

    @Test
    void dropAllItems() {
        List<Integer> result = Heddle.range(0, 5).filter(n -> false).toList();
        assertThat(result).isEmpty();
    }

    @Test
    void keepAllItems() {
        List<Integer> result = Heddle.range(0, 4).filter(n -> true).toList();
        assertThat(result).containsExactly(0, 1, 2, 3);
    }

    @Test
    void emptySourceStaysEmpty() {
        List<Integer> result = Heddle.<Integer>empty().filter(n -> true).toList();
        assertThat(result).isEmpty();
    }

    @Test
    void filterOnStrings() {
        List<String> result = Heddle.of("alpha", "beta", "gamma", "delta")
                .filter(s -> s.startsWith("b") || s.startsWith("d"))
                .toList();
        assertThat(result).containsExactly("beta", "delta");
    }

    @Test
    void filterType() {
        List<String> result = Heddle.of((Object) "hello", 42, "world", 7L)
                .<String>filterType(String.class)
                .toList();
        assertThat(result).containsExactly("hello", "world");
    }

    @Test
    void filterTypeExcludesNonMatching() {
        List<Integer> result = Heddle.of((Object) "a", 1, "b", 2, 3L)
                .<Integer>filterType(Integer.class)
                .toList();
        assertThat(result).containsExactly(1, 2);
    }

    @Test
    void chainedFilters() {
        List<Integer> result = Heddle.range(0, 20)
                .filter(n -> n % 2 == 0)
                .filter(n -> n % 3 == 0)
                .toList();
        assertThat(result).containsExactly(0, 6, 12, 18);
    }

    @Test
    void filterPreservesOrder() {
        List<Integer> result = Heddle.of(5, 3, 1, 4, 2).filter(n -> n > 2).toList();
        assertThat(result).containsExactly(5, 3, 4);
    }
}
