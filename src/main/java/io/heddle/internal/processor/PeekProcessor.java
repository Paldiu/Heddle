package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.Stage;
import io.heddle.api.StageContext;

import java.util.function.Consumer;

/** Runs a side-effect action for each item; the item passes through unchanged. */
public final class PeekProcessor<T> implements Stage<T, T>, Describable {

    private final Consumer<T> action;

    public PeekProcessor(Consumer<T> action) {
        if (action == null) throw new NullPointerException("action must not be null");
        this.action = action;
    }

    @Override
    public void process(Owned<T> item, StageContext<T> ctx) {
        T value = item.consume();
        action.accept(value);
        ctx.emit(value);
    }

    @Override
    public String describe() { return "PeekProcessor"; }
}
