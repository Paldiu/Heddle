package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.StageContext;
import io.heddle.internal.transformer.StatefulTransformer;

import java.time.Duration;

/**
 * Bounds the time spent waiting for a full downstream channel. Each item is
 * forwarded to downstream inside a child virtual thread; if that child blocks
 * for longer than {@code timeout} (typically because the downstream channel is
 * full), the child is interrupted and the item is silently dropped.
 *
 * <p><b>Semantic:</b> this operator limits downstream backpressure blocking,
 * not upstream processing time. Place it between a fast producer and a slow
 * consumer:
 * <pre>{@code
 *   Heddle.generate(fastSource)
 *       .timeout(Duration.ofMillis(50))
 *       .sink(slowConsumer)
 *       .start();
 * }</pre>
 */
public final class TimeoutProcessor<T> extends StatefulTransformer<T, T> implements Describable {

    private static final System.Logger LOGGER =
            System.getLogger(TimeoutProcessor.class.getName());

    private final long timeoutMs;

    public TimeoutProcessor(Duration timeout) {
        if (timeout == null || timeout.isNegative() || timeout.isZero())
            throw new IllegalArgumentException("timeout must be a positive Duration");
        this.timeoutMs = timeout.toMillis();
    }

    @Override
    public void process(Owned<T> item, StageContext<T> ctx) {
        T value = item.consume();
        Thread child = Thread.ofVirtual().start(() -> ctx.emit(value));
        try {
            child.join(timeoutMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (child.isAlive()) {
            child.interrupt();
            try {
                child.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            LOGGER.log(System.Logger.Level.WARNING,
                    "TimeoutProcessor: downstream blocked >{0}ms; item dropped", timeoutMs);
        }
    }

    @Override
    public void flush(StageContext<T> ctx) {}

    @Override
    public String describe() { return "TimeoutProcessor(" + timeoutMs + "ms)"; }
}
