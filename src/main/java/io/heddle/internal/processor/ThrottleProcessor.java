package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.StageContext;
import io.heddle.concurrent.HeddleWheelTimer;
import io.heddle.internal.transformer.StatefulTransformer;

import java.util.concurrent.locks.LockSupport;

/**
 * Limits throughput to at most {@code maxPerSecond} items per second using a
 * fixed one-second window. When the per-window budget is exhausted the stage
 * virtual thread parks via {@link LockSupport#park} until the next allowed
 * emission time, as determined by {@link HeddleWheelTimer}.
 *
 * <p>Unlike the former {@link Thread#sleep} approach, which created one JVM timer
 * event per virtual thread, all delay signals are routed through the shared
 * hashed wheel timer running on a single dedicated platform thread. This avoids
 * the scheduling jitter that occurs under Loom when large numbers of VTs each
 * independently register sleep events with the JVM timer infrastructure.
 *
 * <p><b>Burst note:</b> the fixed-window model allows up to
 * {@code 2 * maxPerSecond} items across a window boundary (all {@code maxPerSecond}
 * items at the tail of one window followed immediately by {@code maxPerSecond} at
 * the head of the next). Pair with {@code batch(n)} downstream if burst smoothing
 * is required.
 */
public final class ThrottleProcessor<T> extends StatefulTransformer<T, T> implements Describable {

    private final int maxPerSecond;
    private long nextAllowedAt = -1L;

    public ThrottleProcessor(int maxPerSecond) {
        if (maxPerSecond <= 0)
            throw new IllegalArgumentException("maxPerSecond must be positive");
        this.maxPerSecond = maxPerSecond;
    }

    @Override
    public void process(Owned<T> item, StageContext<T> ctx) {
        long intervalMs = 1000L / maxPerSecond;
        long now = System.currentTimeMillis();
        long delay = nextAllowedAt - now;
        if (delay > 0) {
            Thread current = Thread.currentThread();
            HeddleWheelTimer.TimerHandle handle =
                    HeddleWheelTimer.INSTANCE.schedule(delay, () -> LockSupport.unpark(current));
            LockSupport.park(this);
            handle.cancel();
        }
        ctx.emit(item.consume());
        nextAllowedAt = System.currentTimeMillis() + intervalMs;
    }

    @Override
    public void flush(StageContext<T> ctx) {}

    @Override
    public void reset() {
        nextAllowedAt = -1L;
    }

    @Override
    public String describe() { return "ThrottleProcessor(" + maxPerSecond + "/s)"; }
}
