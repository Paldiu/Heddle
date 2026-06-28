package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.PipelineHandle;
import io.heddle.api.Sink;
import io.heddle.api.Stage;
import io.heddle.api.StageContext;

/**
 * Async fan-out stage. Delivers each item to a side-branch pipeline running on
 * its own virtual threads, then forwards the item downstream unchanged.
 *
 * <p>Items are written to the branch through a {@link io.heddle.interop.HeddleQueue}
 * bridge, so the main pipeline VT never blocks waiting for the branch to finish.
 * If the branch queue is full the put parks the main VT until capacity returns,
 * providing natural back-pressure.
 *
 * <p>When upstream signals end-of-stream, {@link #flush} calls
 * {@link Sink#onComplete()} on the bridge sink so the branch pipeline drains
 * cleanly instead of hanging on an empty queue.
 *
 * <p>Errors inside the branch are isolated: they fail the branch pipeline but
 * do not propagate to the main pipeline. If the main pipeline is cancelled,
 * {@link io.heddle.internal.PipelineHandleImpl} cancels companion handles —
 * including this branch, as part of the coordinated shutdown.
 *
 * <p>The branch handle is started by {@link io.heddle.HeddleCore} when the main
 * pipeline starts, before the main supervisor VT, so the branch is always
 * listening before the first item arrives.
 */
public final class AsyncTeeProcessor<T> implements Stage<T, T>, Describable {

    private final Sink<T> branchSink;
    private final PipelineHandle branchHandle;

    public AsyncTeeProcessor(Sink<T> branchSink, PipelineHandle branchHandle) {
        if (branchSink   == null) throw new NullPointerException("branchSink must not be null");
        if (branchHandle == null) throw new NullPointerException("branchHandle must not be null");
        this.branchSink   = branchSink;
        this.branchHandle = branchHandle;
    }

    /**
     * Returns the assembled-but-not-yet-started branch handle.
     * Called by {@link io.heddle.HeddleCore} at assembly time to collect
     * companion handles for co-start and co-cancel.
     */
    public PipelineHandle branchHandle() { return branchHandle; }

    @Override
    public void process(Owned<T> item, StageContext<T> ctx) {
        T value = item.consume();
        try {
            branchSink.accept(value);
        } catch (Throwable ignored) {}
        
        ctx.emit(value);
    }

    @Override
    public void flush(StageContext<T> ctx) {
        try {
            branchSink.onComplete();
        } catch (Throwable ignored) {}
    }

    @Override
    public String describe() { return "AsyncTee"; }
}
