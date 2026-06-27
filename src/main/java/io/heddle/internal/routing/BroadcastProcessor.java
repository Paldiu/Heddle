package io.heddle.internal.routing;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.Stage;
import io.heddle.api.StageContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Fan-out stage. Delivers each item to all registered consumers before
 * forwarding it downstream unchanged.
 *
 * <p>Consumers are called sequentially on the stage's virtual thread.
 * A throwing consumer is isolated: its exception is suppressed (collected)
 * and the remaining consumers still execute. After all consumers run, the
 * item is forwarded downstream. If any consumer threw, the first exception
 * is re-thrown (with subsequent ones added as suppressed).
 *
 * <p>For true parallel fan-out (each consumer on its own VT), chain multiple
 * downstream pipelines via {@link io.heddle.Heddle#merge} instead.
 */
public final class BroadcastProcessor<T> implements Stage<T, T>, Describable {

    private final List<Consumer<T>> consumers;

    @SafeVarargs
    public BroadcastProcessor(Consumer<T>... consumers) {
        this.consumers = List.of(consumers);
    }

    public BroadcastProcessor(List<Consumer<T>> consumers) {
        this.consumers = List.copyOf(consumers);
    }

    @Override
    public void process(Owned<T> item, StageContext<T> ctx) {
        T value = item.consume();

        List<Throwable> errors = null;
        for (Consumer<T> consumer : consumers) {
            try {
                consumer.accept(value);
            } catch (Throwable t) {
                if (errors == null) errors = new ArrayList<>();
                errors.add(t);
            }
        }

        
        ctx.emit(value);

        if (errors != null) {
            RuntimeException first = errors.get(0) instanceof RuntimeException re
                    ? re : new RuntimeException(errors.get(0));
            for (int i = 1; i < errors.size(); i++) first.addSuppressed(errors.get(i));
            throw first;
        }
    }

    @Override
    public String describe() { return "BroadcastProcessor(" + consumers.size() + ")"; }
}
