package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.Stage;
import io.heddle.api.StageContext;

import java.util.function.Function;

/**
 * 1:1 transform stage. Applies {@code fn} to each item and emits the result.
 */
public final class MapProcessor<I, O> implements Stage<I, O>, Describable {

    private final Function<I, O> fn;

    public MapProcessor(Function<I, O> fn) {
        if (fn == null) throw new NullPointerException("fn must not be null");
        this.fn = fn;
    }

    @Override
    public void process(Owned<I> item, StageContext<O> ctx) {
        ctx.emit(fn.apply(item.consume()));
    }

    @Override
    public String describe() { return "MapProcessor"; }
}
