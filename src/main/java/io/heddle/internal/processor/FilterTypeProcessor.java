package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.Stage;
import io.heddle.api.StageContext;

/** Keeps only items that are instances of {@code type}; casts each retained item. */
public final class FilterTypeProcessor<I, O> implements Stage<I, O>, Describable {

    private final Class<O> type;

    public FilterTypeProcessor(Class<O> type) {
        if (type == null) throw new NullPointerException("type must not be null");
        this.type = type;
    }

    @Override
    public void process(Owned<I> item, StageContext<O> ctx) {
        I value = item.consume();
        if (type.isInstance(value)) {
            ctx.emit(type.cast(value));
        }
    }

    @Override
    public String describe() { return "FilterTypeProcessor(" + type.getSimpleName() + ")"; }
}
