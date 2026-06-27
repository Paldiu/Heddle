package io.heddle.policies;

import io.heddle.error.HeddleException;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Defines the behaviour applied when a downstream inter-stage channel is at capacity.
 *
 * <p>The default policy is {@link #BLOCK}: the producing virtual thread parks on
 * {@link ArrayBlockingQueue#put(Object)} until a slot becomes available. This is the
 * natural, zero-overhead backpressure mechanism for virtual threads: parking releases
 * the carrier thread, so hundreds of stalled producer stages cost no platform threads.
 *
 * <p>Alternative policies are available for use cases where stalling the producer is
 * unacceptable:
 * <ul>
 *   <li>{@link #DROP} silently discards items when the channel is full.</li>
 *   <li>{@link #FAIL_FAST} signals an immediate pipeline failure.</li>
 * </ul>
 *
 * <p>Apply a policy to a stage via {@link io.heddle.Pipeline#backpressure(BackpressurePolicy)}:
 *
 * <pre>{@code
 * Heddle.from(eventSource)
 *       .backpressure(BackpressurePolicy.DROP)
 *       .map(EventRecord::parse)
 *       .forEach(db::insert);
 * }</pre>
 *
 * <p>Only the three singleton constants ({@link #BLOCK}, {@link #DROP},
 * {@link #FAIL_FAST}) may be used as policies. No public constructors are exposed.
 *
 * @see io.heddle.Pipeline#backpressure(BackpressurePolicy)
 * @see io.heddle.api.NChannel
 */
public abstract sealed class BackpressurePolicy
        permits BackpressurePolicy.Block,
                BackpressurePolicy.Drop,
                BackpressurePolicy.FailFast {

    private BackpressurePolicy() {}

    /**
     * Applies this policy for a single token by either enqueuing it, discarding it, or
     * signalling a pipeline failure.
     *
     * @param <T>         the item type
     * @param token       the item to enqueue or discard
     * @param queue       the target inter-stage channel queue
     * @param dropCounter a counter incremented each time {@link Drop} discards an item;
     *                    reflected in {@link io.heddle.api.StageStats#droppedCount()}
     * @param onFull      a callback invoked with a {@link HeddleException} when
     *                    {@link FailFast} detects a full channel; ignored by {@link Block}
     *                    and {@link Drop}
     * @throws InterruptedException if the calling virtual thread is interrupted while
     *                              blocked; thrown only by {@link Block}
     */
    public abstract <T> void apply(
            T token,
            ArrayBlockingQueue<T> queue,
            AtomicLong dropCounter,
            Consumer<RuntimeException> onFull) throws InterruptedException;

    /**
     * Parks the producing virtual thread until a slot opens in the downstream channel.
     *
     * <p>This is the default backpressure strategy. The parked virtual thread releases
     * its carrier, so throughput of other virtual threads is unaffected. No items are
     * ever lost with this policy.
     */
    public static final class Block extends BackpressurePolicy {

        private Block() {}

        /**
         * Blocks by calling {@link ArrayBlockingQueue#put(Object)}, which parks the
         * calling virtual thread until a slot is available.
         *
         * {@inheritDoc}
         */
        @Override
        public <T> void apply(T token, ArrayBlockingQueue<T> queue,
                AtomicLong dropCounter, Consumer<RuntimeException> onFull)
                throws InterruptedException {
            queue.put(token);
        }
    }

    /**
     * Silently discards items when the downstream channel is at capacity.
     *
     * <p>The producer is never blocked. The drop count is incremented for each discarded
     * item and is reflected in {@link io.heddle.api.StageStats#droppedCount()}.
     */
    public static final class Drop extends BackpressurePolicy {

        private Drop() {}

        /**
         * Uses {@link ArrayBlockingQueue#offer(Object)}, which returns immediately. If
         * the queue is full the item is discarded and {@code dropCounter} is incremented.
         *
         * {@inheritDoc}
         */
        @Override
        public <T> void apply(T token, ArrayBlockingQueue<T> queue,
                AtomicLong dropCounter, Consumer<RuntimeException> onFull) {
            if (!queue.offer(token)) dropCounter.incrementAndGet();
        }
    }

    /**
     * Signals an immediate pipeline failure when the downstream channel is at capacity.
     *
     * <p>The producer is never blocked. If the channel is full, {@code onFull} is
     * invoked with a {@link HeddleException}, propagating as a pipeline failure.
     */
    public static final class FailFast extends BackpressurePolicy {

        private FailFast() {}

        /**
         * Uses {@link ArrayBlockingQueue#offer(Object)}, which returns immediately. If
         * the queue is full, {@code onFull} is called with a {@link HeddleException}.
         *
         * {@inheritDoc}
         */
        @Override
        public <T> void apply(T token, ArrayBlockingQueue<T> queue,
                AtomicLong dropCounter, Consumer<RuntimeException> onFull) {
            if (!queue.offer(token))
                onFull.accept(new HeddleException("channel full: backpressure policy is FailFast"));
        }
    }

    /**
     * The default policy: park the producing virtual thread until a downstream slot
     * opens. No items are ever lost.
     */
    public static final BackpressurePolicy BLOCK     = new Block();

    /**
     * Silently discard items when the downstream channel is full. The producer is never
     * blocked.
     */
    public static final BackpressurePolicy DROP      = new Drop();

    /**
     * Fail the pipeline immediately when the downstream channel is full. The producer
     * is never blocked.
     */
    public static final BackpressurePolicy FAIL_FAST = new FailFast();
}
