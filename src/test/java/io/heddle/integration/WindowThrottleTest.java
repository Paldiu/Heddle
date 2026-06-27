package io.heddle.integration;

import io.heddle.Heddle;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class WindowThrottleTest {

    @Test
    void windowGroupsItemsByTime() {
        List<List<Long>> windows = Heddle.tick(Duration.ofMillis(10))
                .limit(20)
                .window(Duration.ofMillis(50))
                .toList();
        
        assertThat(windows).isNotEmpty();
        long totalItems = windows.stream().mapToLong(List::size).sum();
        assertThat(totalItems).isEqualTo(20);
    }

    @Test
    void windowEmptySourceProducesNoWindows() {
        List<List<Integer>> windows = Heddle.<Integer>empty()
                .window(Duration.ofMillis(50))
                .toList();
        assertThat(windows).isEmpty();
    }

    @Test
    void throttleReducesThroughput() {
        long start = System.currentTimeMillis();
        Heddle.range(0, 5).throttle(10).drain();
        long elapsed = System.currentTimeMillis() - start;
        assertThat(elapsed).isGreaterThanOrEqualTo(400L);
    }
}
