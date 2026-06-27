package io.heddle.integration;

import io.heddle.Heddle;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.*;

class BasicPipelineTest {

    

    @Test
    void ofProducesFixedItems() {
        List<String> result = Heddle.of("a", "b", "c").toList();
        assertThat(result).containsExactly("a", "b", "c");
    }

    @Test
    void emptyProducesNothing() {
        assertThat(Heddle.empty().toList()).isEmpty();
    }

    @Test
    void rangeProducesHalfOpenInterval() {
        List<Integer> result = Heddle.range(2, 7).toList();
        assertThat(result).containsExactly(2, 3, 4, 5, 6);
    }

    @Test
    void rangeEmptyWhenFromEqualsTo() {
        assertThat(Heddle.range(5, 5).toList()).isEmpty();
    }

    @Test
    void fromIterable() {
        List<String> result = Heddle.from(List.of("x", "y", "z")).toList();
        assertThat(result).containsExactly("x", "y", "z");
    }

    @Test
    void fromStream() {
        List<Integer> result = Heddle.fromStream(IntStream.range(0, 4).boxed()).toList();
        assertThat(result).containsExactly(0, 1, 2, 3);
    }

    @Test
    void fromIterator() {
        Iterator<String> it = List.of("m", "n").iterator();
        List<String> result = Heddle.fromIterator(it).toList();
        assertThat(result).containsExactly("m", "n");
    }

    @Test
    void fromSupplierNullSignalsEnd() {
        AtomicInteger counter = new AtomicInteger();
        List<Integer> result = Heddle.from(() -> {
            int v = counter.getAndIncrement();
            return v < 5 ? v : null;
        }).toList();
        assertThat(result).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    void fromEmitter() {
        List<String> result = Heddle.from((io.heddle.api.Emitter<String> emit) -> {
            emit.emit("pushed-1");
            emit.emit("pushed-2");
        }).toList();
        assertThat(result).containsExactly("pushed-1", "pushed-2");
    }

    @Test
    void nullSourceThrows() {
        assertThatThrownBy(() -> Heddle.from((Iterable<Object>) null))
                .isInstanceOf(NullPointerException.class);
    }

    

    @Test
    void count() {
        assertThat(Heddle.range(0, 50).count()).isEqualTo(50);
    }

    @Test
    void firstReturnsFirstItem() {
        assertThat(Heddle.range(0, 10).first()).hasValue(0);
    }

    @Test
    void firstOnEmptyIsEmpty() {
        assertThat(Heddle.<Integer>empty().first()).isEmpty();
    }

    @Test
    void reduce() {
        Optional<Integer> sum = Heddle.range(1, 6).reduce(Integer::sum);
        assertThat(sum).hasValue(15);
    }

    @Test
    void reduceEmptyIsEmpty() {
        assertThat(Heddle.<Integer>empty().reduce(Integer::sum)).isEmpty();
    }

    @Test
    void forEachVisitsAllItems() {
        AtomicInteger sum = new AtomicInteger();
        Heddle.range(1, 4).forEach(sum::addAndGet);
        assertThat(sum.get()).isEqualTo(6);
    }

    @Test
    void toCollection() {
        LinkedList<Integer> result = Heddle.range(0, 3).toCollection(LinkedList::new);
        assertThat(result).containsExactly(0, 1, 2);
    }

    @Test
    void toMap() {
        Map<String, Integer> result = Heddle.of("a", "bb", "ccc")
                .toMap(s -> s, String::length);
        assertThat(result).containsExactlyInAnyOrderEntriesOf(Map.of("a", 1, "bb", 2, "ccc", 3));
    }

    @Test
    void groupBy() {
        Map<Integer, List<String>> result = Heddle.of("a", "bb", "c", "dd")
                .groupBy(String::length);
        assertThat(result.get(1)).containsExactlyInAnyOrder("a", "c");
        assertThat(result.get(2)).containsExactlyInAnyOrder("bb", "dd");
    }

    @Test
    void partition() {
        Map<Boolean, List<Integer>> result = Heddle.range(0, 6)
                .partition(n -> n % 2 == 0);
        assertThat(result.get(true)).containsExactly(0, 2, 4);
        assertThat(result.get(false)).containsExactly(1, 3, 5);
    }

    @Test
    void drainCompletesWithoutError() {
        assertThatNoException().isThrownBy(() -> Heddle.range(0, 1000).drain());
    }

    @Test
    void toFutureCompletesNormally() throws Exception {
        List<Integer> result = Heddle.range(1, 4).map(n -> n * n).toFuture().get();
        assertThat(result).containsExactly(1, 4, 9);
    }

    

    @Test
    void peekDoesNotMutateItems() {
        List<Integer> seen = new ArrayList<>();
        List<Integer> result = Heddle.range(0, 3).peek(seen::add).toList();
        assertThat(seen).containsExactly(0, 1, 2);
        assertThat(result).containsExactly(0, 1, 2);
    }

    @Test
    void log() {
        assertThatNoException().isThrownBy(() ->
                Heddle.range(0, 3).log("test").toList());
    }

    @Test
    void buffer() {
        List<Integer> result = Heddle.range(0, 5).buffer(10).toList();
        assertThat(result).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    void describe() {
        String desc = Heddle.range(0, 5).map(n -> n).filter(n -> true).describe();
        assertThat(desc).startsWith("Source:");
        assertThat(desc).contains("MapProcessor");
        assertThat(desc).contains("FilterProcessor");
    }

    @Test
    void named() {
        assertThatNoException().isThrownBy(() ->
                Heddle.range(0, 5).named("my-pipeline").toList());
    }

    @Test
    void chained() {
        List<String> result = Heddle.range(1, 6)
                .filter(n -> n % 2 == 1)
                .map(n -> n * n)
                .map(Object::toString)
                .toList();
        assertThat(result).containsExactly("1", "9", "25");
    }
}
