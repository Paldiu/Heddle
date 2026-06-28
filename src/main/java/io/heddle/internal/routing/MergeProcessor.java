package io.heddle.internal.routing;

import io.heddle.api.NChannel;
import io.heddle.api.StageContext;
import io.heddle.context.PipelineContext;
import io.heddle.error.HeddleException;
import io.heddle.internal.Transfer;
import io.heddle.util.ThreadUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Fan-in source stage. Reads from N upstream channels concurrently and emits
 * each item to a single downstream context as it arrives.
 *
 * <p>Each upstream channel is drained on its own child virtual thread. The stage
 * completes when ALL upstreams have sent {@link Transfer.Complete}. Items from
 * different sources interleave in arrival order; no ordering guarantee across sources.
 */
@SuppressWarnings("unchecked")
public final class MergeProcessor<T> implements Runnable {

    private final List<NChannel<Transfer<T>>> upstreams;
    private final StageContext<T> downstream;
    private final PipelineContext context;
    private final String stageId;

    public MergeProcessor(
            String stageId,
            List<NChannel<Transfer<T>>> upstreams,
            StageContext<T> downstream,
            PipelineContext context) {
        this.stageId    = stageId;
        this.upstreams  = List.copyOf(upstreams);
        this.downstream = downstream;
        this.context    = context;
    }

    @Override
    public void run() {
        context.registerStage(stageId, Thread.currentThread());
        CountDownLatch latch = new CountDownLatch(upstreams.size());

        List<Thread> drainers = new ArrayList<>(upstreams.size());
        int i = 0;
        for (NChannel<Transfer<T>> upstream : upstreams) {
            final NChannel<Transfer<T>> src = upstream;
            drainers.add(ThreadUtils.startVirtual(stageId + "-drainer-" + i++, () -> drain(src, latch)));
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {   
            ThreadUtils.joinAll(drainers.toArray(new Thread[0]));
        }
    }

    private void drain(NChannel<Transfer<T>> upstream, CountDownLatch latch) {
        try {
            loop: while (context.isRunning()) {
                Transfer<T> token = upstream.take();
                switch (token) {
                    case Transfer.Complete<?> _ -> { break loop; }
                    case Transfer.Signal<?, ?> sig -> downstream.emitSignal(sig.payload());
                    case Transfer.Ready<?> r -> {
                        T value = (T) r.value();
                        downstream.emit(value);
                    }
                }
            }
        } 
        catch (HeddleException ignored) {} 
        finally {
            latch.countDown();
        }
    }
}
