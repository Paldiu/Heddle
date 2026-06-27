package io.heddle.interop;

import io.heddle.Heddle;
import io.heddle.Pipeline;
import io.heddle.api.Emitter;
import io.heddle.api.Sink;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Thread-safe bridge between two independent Heddle pipelines.
 *
 * <p>One pipeline writes into the queue via {@link #asSink()}; a second pipeline reads
 * from it via {@link #asSource()}. The bridge decouples the producer and consumer
 * pipelines so they can run at different rates, each on their own virtual threads,
 * without either blocking the other beyond the configured capacity.
 *
 * <pre>{@code
 * HeddleQueue<String> bridge = new HeddleQueue<>(256);
 *
 * // Producer pipeline
 * Heddle.fromLines(path)
 *       .map(String::toUpperCase)
 *       .sink(bridge.asSink())
 *       .start();
 *
 * // Consumer pipeline (completes when the producer finishes)
 * bridge.asSource()
 *       .filter(s -> s.startsWith("A"))
 *       .forEach(System.out::println);
 * }</pre>
 *
 * <p>When the producer pipeline completes normally, the source signals end-of-stream
 * and the consumer pipeline finishes cleanly. If the producer fails, the error is
 * propagated to the consumer pipeline as a pipeline failure after the last queued item
 * has been delivered.
 *
 * <p>A single {@code HeddleQueue} supports exactly one producer and one consumer.
 * Concurrent producers or consumers will interleave items non-deterministically.
 *
 * @param <T> the type of items transferred through this queue
 */
public final class HeddleQueue<T> {

    private static final Object EOF = new Object();

    private final LinkedBlockingQueue<Object> queue;
    private volatile Throwable upstreamError;

    /**
     * Creates an unbounded queue backed by {@link Integer#MAX_VALUE} capacity.
     *
     * <p>Because the queue is effectively unbounded, the producer pipeline will never
     * block waiting for the consumer to catch up. For natural backpressure, use
     * {@link #HeddleQueue(int)} with an explicit capacity instead.
     */
    public HeddleQueue() {
        this(Integer.MAX_VALUE);
    }

    /**
     * Creates a queue with the given bounded capacity.
     *
     * <p>The producer pipeline blocks when the queue is full, providing natural
     * backpressure that slows the producer to the consumer's pace without dropping
     * items.
     *
     * @param capacity the maximum number of items the queue may hold at one time;
     *                 must be positive
     * @throws IllegalArgumentException if {@code capacity} is not positive
     */
    public HeddleQueue(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be positive");
        this.queue = new LinkedBlockingQueue<>(capacity);
    }

    /**
     * Returns a {@link Sink} that feeds items into this queue.
     *
     * <p>Pass the returned sink to {@link Pipeline#sink(Sink)} on the producer pipeline.
     * When the producer pipeline completes normally, the sink signals end-of-stream to
     * the consumer. If the producer fails, the sink records the error so it can be
     * propagated to the consumer after the last queued item is delivered.
     *
     * @return a {@code Sink} that enqueues each accepted item into this queue
     */
    public Sink<T> asSink() {
        return new Sink<T>() {
            @Override
            public void accept(T item) {
                try {
                    queue.put(item);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void onComplete() {
                putEof();
            }

            @Override
            public void onError(Throwable cause) {
                upstreamError = cause;
                putEof();
            }
        };
    }

    /**
     * Returns a {@link Pipeline} that drains this queue as its source.
     *
     * <p>The returned pipeline blocks on each {@link #take()} until the producer
     * delivers an item or signals end-of-stream. If the producer signalled a failure,
     * a {@link RuntimeException} is thrown on the consumer pipeline after the last
     * queued item has been delivered.
     *
     * @return a {@code Pipeline} whose source is this queue
     */
    @SuppressWarnings("unchecked")
    public Pipeline<T> asSource() {
        return Heddle.from((Emitter<T> emitter) -> {
            while (true) {
                Object item;
                try {
                    item = queue.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                if (item == EOF) {
                    Throwable err = upstreamError;
                    if (err instanceof RuntimeException re) throw re;
                    if (err != null) throw new RuntimeException("upstream pipeline failed", err);
                    break;
                }
                emitter.emit((T) item);
            }
        }).describedAs("HeddleQueue");
    }

    /**
     * Returns the number of items currently waiting in the queue.
     *
     * <p>This count excludes the internal end-of-stream sentinel and reflects the
     * number of items the consumer has not yet retrieved.
     *
     * @return the number of items currently in the queue
     */
    public int size() {
        return queue.size();
    }

    /**
     * Returns the number of additional items the queue can accept before the producer
     * pipeline will block.
     *
     * @return the remaining capacity of this queue
     */
    public int remainingCapacity() {
        return queue.remainingCapacity();
    }

    private void putEof() {
        try {
            queue.put(EOF);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
