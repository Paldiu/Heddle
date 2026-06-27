package io.heddle.internal;

import io.heddle.api.NChannel;
import io.heddle.api.StageContext;
import io.heddle.concurrent.AdmissionController;
import io.heddle.concurrent.ChildThreadTracker;
import io.heddle.context.PipelineContext;
import io.heddle.error.HeddleException;

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
 * semantics — the expected contract for async fan-out.
 *
 * <p><b>Graceful completion:</b> on receiving {@code Transfer.Complete} from upstream,
 * this stage calls {@link ChildThreadTracker#awaitAll()} to drain all in-flight children
 * before forwarding the completion sentinel downstream.
 *
 * <p><b>Failure / cancellation (victim path):</b> if a {@link HeddleException} is thrown
 * by a channel operation, the context is already terminal. In-flight children are
 * interrupted via {@link ChildThreadTracker#interruptAll()} and completion is forwarded
 * immediately; children will exit on their next channel op.
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
        this.stageId    = stageId;
        this.upstream   = upstream;
        this.downstream = downstream;
        this.fn         = fn;
        this.context    = context;
        this.admission  = new AdmissionController(concurrency);
        this.tracker    = new ChildThreadTracker();
    }

    @Override
    public void run() {
        context.registerStage(stageId, Thread.currentThread());
        StageContext<O> stageCtx = value -> downstream.put(new Transfer.Ready<>(value));
        try {
            while (context.isRunning()) {
                Transfer<I> transfer = upstream.take();

                if (transfer instanceof Transfer.Complete<?>) {
                    tracker.awaitAll();
                    break;
                }

                if (transfer instanceof Transfer.Ready<?> ready) {
                    @SuppressWarnings("unchecked")
                    I value = (I) ready.value();

                    admission.acquire();

                    tracker.trackAndStart(stageId + "-child-" + childIndex.getAndIncrement(), () -> {
                        try {
                            stageCtx.emitAll(fn.apply(value));
                        } catch (HeddleException he) {
                            // Another stage already signaled failure so we just exit here.
                        } catch (Throwable t) {
                            context.signalFailure(stageId, t);
                        } finally {
                            admission.release();
                            tracker.untrack(Thread.currentThread());
                        }
                    });
                }
            }
        } catch (HeddleException victim) {
            tracker.interruptAll();
        } catch (Throwable fatal) {
            context.signalFailure(stageId, fatal);
            tracker.interruptAll();
        } finally {
            Thread.interrupted(); 
            forwardCompletion();
        }
    }

    private void forwardCompletion() {
        try {
            downstream.put(Transfer.complete());
        } catch (RuntimeException ignored) {}
    }
}
