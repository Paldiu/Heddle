package io.heddle.integration;

import io.heddle.Heddle;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class BroadcastIntegrationTest {

    @Test
    void broadcastDeliversCopyToAllConsumers() {
        List<Integer> branch1 = Collections.synchronizedList(new ArrayList<>());
        List<Integer> branch2 = Collections.synchronizedList(new ArrayList<>());

        Heddle.range(0, 5)
                .broadcast(branch1::add, branch2::add)
                .drain();

        assertThat(branch1).containsExactly(0, 1, 2, 3, 4);
        assertThat(branch2).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    void broadcastContinuesDownstreamAfterFanOut() {
        List<Integer> side = Collections.synchronizedList(new ArrayList<>());
        List<Integer> result = Heddle.range(0, 5)
                .broadcast(side::add)
                .map(n -> n * 2)
                .toList();

        assertThat(side).containsExactly(0, 1, 2, 3, 4);
        assertThat(result).containsExactly(0, 2, 4, 6, 8);
    }

    @Test
    void broadcastEmptySource() {
        List<Integer> branch = Collections.synchronizedList(new ArrayList<>());
        Heddle.<Integer>empty().broadcast(branch::add).drain();
        assertThat(branch).isEmpty();
    }

    @Test
    void broadcastSingleConsumer() {
        List<String> seen = Collections.synchronizedList(new ArrayList<>());
        List<String> result = Heddle.of("a", "b", "c")
                .broadcast(seen::add)
                .toList();
        assertThat(seen).containsExactly("a", "b", "c");
        assertThat(result).containsExactly("a", "b", "c");
    }

    @Test
    void broadcastThreeConsumers() {
        List<Integer> a = Collections.synchronizedList(new ArrayList<>());
        List<Integer> b = Collections.synchronizedList(new ArrayList<>());
        List<Integer> c = Collections.synchronizedList(new ArrayList<>());
        Heddle.range(1, 4).broadcast(a::add, b::add, c::add).drain();
        assertThat(a).containsExactly(1, 2, 3);
        assertThat(b).containsExactly(1, 2, 3);
        assertThat(c).containsExactly(1, 2, 3);
    }
}
