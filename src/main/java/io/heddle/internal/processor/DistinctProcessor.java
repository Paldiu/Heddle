package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.StageContext;
import io.heddle.internal.transformer.StatefulTransformer;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Drops items whose key has already been seen.
 *
 * <p>When constructed without {@code maxKeys} (or with {@code maxKeys = 0}) the
 * seen-key set is unbounded; a {@link System.Logger} warning is emitted once when
 * the set reaches 100 000 entries. Pass a positive {@code maxKeys} to cap memory:
 *
 * <ul>
 *   <li><b>LRU eviction</b> (default): when the cap is reached the least-recently-seen
 *       key is evicted. The stream continues; recent items are still deduplicated
 *       against the most recent {@code maxKeys} distinct keys.</li>
 *   <li><b>Fail-fast</b> ({@code failFast = true}): an {@link IllegalStateException}
 *       is thrown when a new key would exceed the cap. Use this when silent eviction
 *       would violate a correctness invariant.</li>
 * </ul>
 */
public final class DistinctProcessor<T, K> extends StatefulTransformer<T, T> implements Describable {

    private static final int WARN_THRESHOLD = 100_000;
    private static final System.Logger LOGGER = System.getLogger(DistinctProcessor.class.getName());

    private final Function<T, K> keyExtractor;
    private final int maxKeys;
    private final boolean failFast;
    private final Set<K> seen;

    @SuppressWarnings("unchecked")
    public DistinctProcessor() {
        this(item -> (K) item, 0, false);
    }

    public DistinctProcessor(Function<T, K> keyExtractor) {
        this(keyExtractor, 0, false);
    }

    @SuppressWarnings("unchecked")
    public DistinctProcessor(int maxKeys) {
        this(item -> (K) item, maxKeys, false);
    }

    @SuppressWarnings("unchecked")
    public DistinctProcessor(int maxKeys, boolean failFast) {
        this(item -> (K) item, maxKeys, failFast);
    }

    public DistinctProcessor(Function<T, K> keyExtractor, int maxKeys) {
        this(keyExtractor, maxKeys, false);
    }

    public DistinctProcessor(Function<T, K> keyExtractor, int maxKeys, boolean failFast) {
        if (keyExtractor == null) throw new NullPointerException("keyExtractor must not be null");
        if (maxKeys < 0) throw new IllegalArgumentException("maxKeys must be non-negative");
        this.keyExtractor = keyExtractor;
        this.maxKeys      = maxKeys;
        this.failFast     = failFast;
        this.seen         = buildSeen(maxKeys, failFast);
    }

    private static <K> Set<K> buildSeen(int maxKeys, boolean failFast) {
        if (maxKeys <= 0 || failFast) return new HashSet<>();
        return Collections.newSetFromMap(new LinkedHashMap<K, Boolean>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, Boolean> eldest) {
                return size() > maxKeys;
            }
        });
    }

    @Override
    public void process(Owned<T> item, StageContext<T> ctx) {
        T value = item.consume();
        K key   = keyExtractor.apply(value);
        if (failFast && maxKeys > 0 && !seen.contains(key) && seen.size() >= maxKeys) {
            throw new IllegalStateException(
                    "DistinctProcessor key set exceeded maxKeys=" + maxKeys);
        }
        if (seen.add(key)) {
            if (maxKeys == 0 && seen.size() == WARN_THRESHOLD) {
                LOGGER.log(System.Logger.Level.WARNING,
                        "DistinctProcessor has accumulated {0} keys without a bound; " +
                        "consider using DistinctProcessor(maxKeys) to cap memory growth",
                        WARN_THRESHOLD);
            }
            ctx.emit(value);
        }
    }

    @Override
    public void flush(StageContext<T> ctx) {}

    @Override
    public void reset() {
        seen.clear();
    }

    @Override
    public String describe() {
        if (maxKeys > 0) {
            return failFast
                    ? "DistinctProcessor(maxKeys=" + maxKeys + ",failFast)"
                    : "DistinctProcessor(maxKeys=" + maxKeys + ")";
        }
        return "DistinctProcessor";
    }
}
