package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.Stage;
import io.heddle.api.StageContext;

import java.util.function.Predicate;

/**
 * Conditional pass-through. Items for which the predicate returns {@code false}
 * are silently dropped; all others are forwarded unchanged.
 */
public final class FilterProcessor<T> implements Stage<T, T>, Describable {

    private final Predicate<T> predicate;

    public FilterProcessor(Predicate<T> predicate) {
        if (predicate == null) throw new NullPointerException("predicate must not be null");
        this.predicate = predicate;
    }

    @Override
    public void process(Owned<T> item, StageContext<T> ctx) {
        T value = item.consume();
        if (predicate.test(value)) {
            ctx.emit(value);
        }
    }

    @Override
    public String describe() { return "FilterProcessor"; }
}
