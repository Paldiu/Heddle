package io.heddle.unit;

import io.heddle.Heddle;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class SkipLimitTest {

    @Test
    void skipLeadingItems() {
        List<Integer> result = Heddle.range(0, 5).skip(2).toList();
        assertThat(result).containsExactly(2, 3, 4);
    }

    @Test
    void skipZeroItems() {
        List<Integer> result = Heddle.range(0, 3).skip(0).toList();
        assertThat(result).containsExactly(0, 1, 2);
    }

    @Test
    void skipMoreThanTotal() {
        List<Integer> result = Heddle.range(0, 3).skip(10).toList();
        assertThat(result).isEmpty();
    }

    @Test
    void skipExactlyAll() {
        List<Integer> result = Heddle.range(0, 5).skip(5).toList();
        assertThat(result).isEmpty();
    }

    @Test
    void limitItems() {
        List<Integer> result = Heddle.range(0, 100).limit(5).toList();
        assertThat(result).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    void limitZero() {
        List<Integer> result = Heddle.range(0, 10).limit(0).toList();
        assertThat(result).isEmpty();
    }

    @Test
    void limitBeyondSourceSize() {
        List<Integer> result = Heddle.range(0, 3).limit(100).toList();
        assertThat(result).containsExactly(0, 1, 2);
    }

    @Test
    void limitNegativeThrows() {
        assertThatThrownBy(() -> Heddle.range(0, 3).limit(-1).toList())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void skipThenLimit() {
        List<Integer> result = Heddle.range(0, 10).skip(3).limit(4).toList();
        assertThat(result).containsExactly(3, 4, 5, 6);
    }

    @Test
    void limitThenSkip() {
        List<Integer> result = Heddle.range(0, 10).limit(5).skip(2).toList();
        assertThat(result).containsExactly(2, 3, 4);
    }

    @Test
    void limitStopsInfiniteSourceCleanly() {
        long count = Heddle.generate(() -> 1).limit(50).count();
        assertThat(count).isEqualTo(50);
    }
}
