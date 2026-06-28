package io.heddle.internal;

import io.heddle.api.NChannel;
import io.heddle.api.StageContext;
import io.heddle.concurrent.AdmissionController;
import io.heddle.concurrent.ChildThreadTracker;
import io.heddle.context.PipelineContext;
import io.heddle.error.HeddleException;
import io.heddle.memory.MemoryGuard;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Stage runnable that handles {@code flatMap}/{@code mapAsync} with concurrency > 1.
 *
 * <p>Unlike {@link PipelineStage}, which calls a stage's {@code process()} synchronously
 * one item at a time, this runnable maintains up to {@code concurrency} items in-flight
 * simultaneously by launching a child virtual thread per item. The
 * {@link AdmissionController} semaphore gates admission so that at most
 * {@code concurrency} children run at once; new items are read from upstream and
 * dispatched without waiting for earlier children to complete.
 *
 * <p><b>Output ordering:</b> child threads write to downstream concurrently, so outputs
 * from item N may arrive before outputs from item N-1. This is merge (unordered)
 * semantics; the expected contract for async fan-out.
 *
 * <p><b>Graceful completion:</b> on receiving {@code Transfer.Complete} from upstream,
 * this stage calls {@link ChildThreadTracker#awaitAll()} to drain all in-flight children
 * before forwarding the completion sentinel downstream. This guarantees all child
 * emissions have been fully written to the downstream channel before the sentinel arrives.
 *
 * <p><b>Failure / cancellation (victim or originator path):</b> children are interrupted
 * via {@link ChildThreadTracker#interruptAll()} and then drained via
 * {@link ChildThreadTracker#awaitAll()} before the completion sentinel is forwarded.
 * The drain step is essential: without it the sentinel can overtake in-flight writes
 * that children have already started but not yet completed, producing out-of-order
 * data in the downstream channel.
 */
public final class ConcurrentFlatMapStage<I, O> implements Runnable {

    private final String stageId;
    private final NChannel<Transfer<I>> upstream;
    private final NChannel<Transfer<O>> downstream;
    private final Function<I, Iterable<O>> fn;
    private final PipelineContext context;
    private final AdmissionController admission;
    private final ChildThreadTracker tracker;
    private final AtomicInteger childIndex = new AtomicInteger();

    public ConcurrentFlatMapStage(
            String stageId,
            NChannel<Transfer<I>> upstream,
            NChannel<Transfer<O>> downstream,
            Function<I, Iterable<O>> fn,
            int concurrency,
            PipelineContext context) {
        this(stageId, upstream, downstream, fn, concurrency, context, null);
    }

    public ConcurrentFlatMapStage(
            String stageId,
            NChannel<Transfer<I>> upstream,
            NChannel<Transfer<O>> downstream,
            Function<I, Iterable<O>> fn,
            int concurrency,
            PipelineContext context,
            MemoryGuard memoryGuard) {
        this.stageId    = stageId;
        this.upstream   = upstream;
        this.downstream = downstream;
        this.fn         = fn;
        this.context    = context;
        this.admission  = new AdmissionController(
                concurrency, AdmissionController.DEFAULT_ACQUIRE_TIMEOUT_MS, memoryGuard);
        this.tracker    = new ChildThreadTracker();
    }

    @Override
    public void run() {
        context.registerStage(stageId, Thread.currentThread());
        StageContext<O> stageCtx = new StageContext<>() {
            @Override public void emit(O value) { downstream.put(new Transfer.Ready<>(value)); }
            @Override public <U> void emitSignal(U payload) { downstream.put(Transfer.signal(payload)); }
        };
        try {
            while (context.isRunning()) {
                Transfer<I> transfer = upstream.take();

                if (transfer instanceof Transfer.Complete<?>) {
                    tracker.awaitAll();
                    break;
                }

                if (transfer instanceof Transfer.Signal<?, ?> sig) {
                    downstream.put(Transfer.signal(sig.payload()));
                    continue;
                }

                if (transfer instanceof Transfer.Ready<?> ready) {
                    @SuppressWarnings("unchecked")
                    I value = (I) ready.value();

                    int stripe = admission.acquire();

                    tracker.trackAndStart(stageId + "-child-" + childIndex.getAndIncrement(), () -> {
                        try {
                            stageCtx.emitAll(fn.apply(value));
                        } catch (HeddleException he) {
                            // Distinguish channel-level signals from user-code exceptions:
                            // if the context is still running, the HeddleException came from
                            // fn.apply() itself (not from a downstream put reacting to a terminal
                            // signal), so it must be escalated as a real failure.
                            if (context.isRunning()) {
                                context.signalFailure(stageId, he);
                            }
                        } catch (Throwable t) {
                            context.signalFailure(stageId, t);
                        } finally {
                            admission.release(stripe);
                            tracker.untrack(Thread.currentThread());
                        }
                    });
                }
            }
        } catch (HeddleException victim) {
            // Pipeline already terminal from another stage; stop children and drain
            // before forwarding the sentinel so no child write can overtake it.
            tracker.interruptAll();
            tracker.awaitAll();
        } catch (Throwable fatal) {
            context.signalFailure(stageId, fatal);
            tracker.interruptAll();
            tracker.awaitAll();
        } finally {
            // Record and restore the interrupt flag: Thread.interrupted() must not
            // silently consume a signal that Loom or the caller may still need.
            boolean wasInterrupted = Thread.interrupted();
            forwardCompletion();
            if (wasInterrupted) Thread.currentThread().interrupt();
        }
    }

    private void forwardCompletion() {
        try {
            downstream.put(Transfer.complete());
        } catch (RuntimeException ignored) {}
    }
}
