package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.Stage;
import io.heddle.api.StageContext;

/** Discards the first {@code n} items then passes all remaining items through. */
public final class SkipProcessor<T> implements Stage<T, T>, Describable {

    private final long n;
    private long skipped = 0;

    public SkipProcessor(long n) {
        if (n < 0) throw new IllegalArgumentException("n must be non-negative");
        this.n = n;
    }

    @Override
    public void process(Owned<T> item, StageContext<T> ctx) {
        T value = item.consume();
        if (skipped < n) {
            skipped++;
        } else {
            ctx.emit(value);
        }
    }

    @Override
    public String describe() { return "SkipProcessor(" + n + ")"; }
}
