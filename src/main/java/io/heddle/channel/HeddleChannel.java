package io.heddle.channel;

import io.heddle.api.NChannel;
import io.heddle.context.PipelineContext;
import io.heddle.error.HeddleException;
import io.heddle.internal.Transfer;
import io.heddle.policies.BackpressurePolicy;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Context-aware, interrupt-correct queue wrapper. Implements {@link NChannel} over an
 * {@link ArrayBlockingQueue} of {@link Transfer} tokens.
 *
 * <p>ABQ blocks on a {@code ReentrantLock}; virtual threads unmount cleanly on
 * j.u.c locks (no carrier pinning). Each call checks the pipeline signal before
 * parking and reclassifies any {@link InterruptedException} through the context
 * so the caller always sees a typed exception, never a raw interrupt.
 *
 * <p>The {@link BackpressurePolicy} governs what happens when the queue is full:
 * <ul>
 *   <li>{@link BackpressurePolicy.Block} (default) - park until space opens.</li>
 *   <li>{@link BackpressurePolicy.Drop}  - silently discard the item; increment a drop counter.</li>
 *   <li>{@link BackpressurePolicy.FailFast} - signal pipeline failure immediately.</li>
 * </ul>
 */
public final class HeddleChannel<T> implements NChannel<Transfer<T>> {

    private static final String SOURCE_STAGE = "channel";

    private final ArrayBlockingQueue<Transfer<T>> queue;
    private final PipelineContext context;
    private final BackpressurePolicy policy;
    private final AtomicLong droppedCount = new AtomicLong();

    /** Constructs a channel with the default {@link BackpressurePolicy#BLOCK} policy. */
    public HeddleChannel(int capacity, PipelineContext context) {
        this(capacity, context, BackpressurePolicy.BLOCK);
    }

    public HeddleChannel(int capacity, PipelineContext context, BackpressurePolicy policy) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be positive");
        this.queue   = new ArrayBlockingQueue<>(capacity);
        this.context = context;
        this.policy  = policy;
    }

    @Override
    public void put(Transfer<T> token) {
        context.checkSignal();
        try {
            if (token instanceof Transfer.Complete<?>) {
                queue.put(token);
            } else {
                policy.apply(token, queue, droppedCount,
                        ex -> context.signalFailure(SOURCE_STAGE, ex));
            }
        } catch (InterruptedException e) {
            context.checkSignal();
            Thread.currentThread().interrupt();
            throw new HeddleException("channel put interrupted", e);
        }
    }

    @Override
    public Transfer<T> take() {
        context.checkSignal();
        try {
            return queue.take();
        } catch (InterruptedException e) {
            context.checkSignal();
            Thread.currentThread().interrupt();
            throw new HeddleException("channel take interrupted", e);
        }
    }

    /** Non-blocking drain invoked during cleanup; releases resource-bearing payloads. */
    public void drainAndRelease(Consumer<T> releaser) {
        Transfer<T> token;
        while ((token = queue.poll()) != null) {
            if (token instanceof Transfer.Ready<T> r) releaser.accept(r.value());
        }
    }

    public int size()            { return queue.size(); }
    public boolean isEmpty()     { return queue.isEmpty(); }
    public long droppedCount()   { return droppedCount.get(); }
    public BackpressurePolicy policy() { return policy; }
}
