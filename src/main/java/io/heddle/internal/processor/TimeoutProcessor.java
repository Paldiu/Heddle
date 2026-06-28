package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.StageContext;
import io.heddle.concurrent.HeddleWheelTimer;
import io.heddle.internal.transformer.StatefulTransformer;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;

/**
 * Bounds the time spent waiting for a full downstream channel. Each item is
 * forwarded to downstream inside a child virtual thread; if the child blocks
 * for longer than {@code timeout} (typically because the downstream channel is
 * full), the wheel timer fires, interrupts the child, and unparks this stage
 * thread so it can continue.
 *
 * <p>Compared to the former {@link Thread#join(long)} approach, which registered
 * one JVM timer event per in-flight item, all eviction signals now route through
 * the single shared {@link HeddleWheelTimer}. Under high item rates this reduces
 * timer-infrastructure contention and eliminates the scheduling jitter that
 * appeared under Loom when thousands of VTs each independently parked on timed
 * joins.
 *
 * <p><b>Semantic:</b> this operator limits downstream backpressure blocking,
 * not upstream processing time. Place it between a fast producer and a slow consumer:
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
        Thread stage = Thread.currentThread();

        Thread child = Thread.ofVirtual().start(() -> {
            ctx.emit(value);
            LockSupport.unpark(stage);
        });

        HeddleWheelTimer.TimerHandle handle = HeddleWheelTimer.INSTANCE.schedule(timeoutMs, () -> {
            child.interrupt();
            LockSupport.unpark(stage);
        });

        LockSupport.park(this);
        handle.cancel();

        if (child.isAlive()) {
            try { child.join(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            if (child.isAlive()) {
                LOGGER.log(System.Logger.Level.WARNING,
                        "TimeoutProcessor: downstream blocked >{0}ms; item dropped", timeoutMs);
            }
        }
    }

    @Override
    public void flush(StageContext<T> ctx) {}

    @Override
    public String describe() { return "TimeoutProcessor(" + timeoutMs + "ms)"; }
}
