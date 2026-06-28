package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.Stage;
import io.heddle.api.StageContext;
import io.heddle.concurrent.AdmissionController;
import io.heddle.concurrent.ChildThreadTracker;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * 1:N expansion stage with bounded concurrency.
 *
 * <p>When {@code concurrency == 1} the expansion runs sequentially on the
 * stage thread; no child VTs are created.
 *
 * <p>When {@code concurrency > 1} each input item is handed to a child
 * virtual thread that runs {@code fn.apply(item)} and emits all outputs.
 * The {@link AdmissionController} bounds how many children are in-flight
 * simultaneously. Children are tracked via {@link ChildThreadTracker} so
 * that when the parent stage is interrupted (cancel/failure) the interrupt
 * cascades to all in-flight children, preventing deadlock against a full
 * downstream queue (C4).
 *
 * <p><b>Track-before-start:</b> each child is registered in the tracker
 * before it starts via {@link ChildThreadTracker#trackAndStart}. This closes
 * the race where a fast child calls {@code untrack} before the parent calls
 * {@code track}, which would leave a phantom entry and cause a join miss.
 *
 * <p>The parent waits for all child threads to finish before returning,
 * preserving the stage lifecycle contract and propagating any child exception
 * back to the pipeline context.
 */
public final class FlatMapProcessor<I, O> implements Stage<I, O>, Describable {

    private final Function<I, Iterable<O>> fn;
    private final int concurrency;
    private final AdmissionController admission;
    private final ChildThreadTracker tracker;
    private final AtomicInteger childIndex = new AtomicInteger();

    public FlatMapProcessor(Function<I, Iterable<O>> fn, int concurrency) {
        if (fn == null)       throw new NullPointerException("fn must not be null");
        if (concurrency <= 0) throw new IllegalArgumentException("concurrency must be positive");
        this.fn        = fn;
        this.concurrency = concurrency;
        this.admission = new AdmissionController(concurrency);
        this.tracker   = new ChildThreadTracker();
    }

    public Function<I, Iterable<O>> fn() { return fn; }
    public int concurrency()              { return concurrency; }

    @Override
    public void process(Owned<I> item, StageContext<O> ctx) {
        I value = item.consume();

        if (concurrency == 1) {
            ctx.emitAll(fn.apply(value));
            return;
        }

        AtomicReference<Throwable> childError = new AtomicReference<>();
        String childName = "flatmap-child-" + childIndex.getAndIncrement();

        int stripe = admission.acquire();

        tracker.trackAndStart(childName, () -> {
            try {
                ctx.emitAll(fn.apply(value));
            } catch (Throwable t) {
                childError.compareAndSet(null, t);
            } finally {
                admission.release(stripe);
                tracker.untrack(Thread.currentThread());
            }
        });

        try {
            tracker.awaitAll();
        } catch (RuntimeException e) {
            tracker.interruptAll();
            throw e;
        }

        Throwable err = childError.get();
        if (err instanceof RuntimeException re) throw re;
        if (err != null) throw new RuntimeException(err);
    }

    @Override
    public String describe() { return "FlatMapProcessor(concurrency=" + concurrency + ")"; }
}
