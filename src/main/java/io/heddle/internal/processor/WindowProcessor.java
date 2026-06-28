package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.StageContext;
import io.heddle.concurrent.HeddleWheelTimer;
import io.heddle.internal.transformer.StatefulTransformer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Collects items into time-bounded windows of type {@code List<T>}. The
 * {@link HeddleWheelTimer} sets a flush flag every {@code duration}; the stage
 * thread checks the flag before processing each item and drains the current
 * buffer when set.
 *
 * <p>The timer handle is acquired lazily on the first item and cancelled when
 * upstream completes. A partial window accumulated after the last timer tick is
 * flushed by {@link #flush}.
 *
 * <p>Compared to the former companion-VT design (which used {@link Thread#sleep}
 * in a separate virtual thread), this implementation routes all tick signals
 * through the shared {@link HeddleWheelTimer}. No additional virtual threads are
 * spawned per window stage, eliminating per-VT sleep jitter under Loom.
 *
 * <p>Unlike {@code batch(n)}, windows are time-driven rather than count-driven:
 * a window closes after {@code duration} regardless of how many items it holds.
 * An empty window (no items arrived during {@code duration}) produces no output.
 */
public final class WindowProcessor<T> extends StatefulTransformer<T, List<T>> implements Describable {

    private final Duration     duration;
    private final AtomicBoolean flushFlag = new AtomicBoolean(false);
    private final List<T>      buffer    = new ArrayList<>();
    private volatile HeddleWheelTimer.TimerHandle timerHandle;

    public WindowProcessor(Duration duration) {
        if (duration == null || duration.isNegative() || duration.isZero())
            throw new IllegalArgumentException("duration must be a positive Duration");
        this.duration = duration;
    }

    @Override
    public void process(Owned<T> item, StageContext<List<T>> ctx) {
        if (timerHandle == null) {
            timerHandle = scheduleFlush();
        }
        if (flushFlag.compareAndSet(true, false)) {
            timerHandle = scheduleFlush();
            if (!buffer.isEmpty()) {
                ctx.emit(new ArrayList<>(buffer));
                buffer.clear();
            }
        }
        buffer.add(item.consume());
    }

    private HeddleWheelTimer.TimerHandle scheduleFlush() {
        return HeddleWheelTimer.INSTANCE.schedule(duration.toMillis(), () -> flushFlag.set(true));
    }

    @Override
    public void flush(StageContext<List<T>> ctx) {
        cancelTimer();
        if (!buffer.isEmpty()) {
            ctx.emit(new ArrayList<>(buffer));
            buffer.clear();
        }
    }

    @Override
    public void reset() {
        cancelTimer();
        flushFlag.set(false);
        buffer.clear();
    }

    private void cancelTimer() {
        HeddleWheelTimer.TimerHandle h = timerHandle;
        if (h != null) {
            h.cancel();
            timerHandle = null;
        }
    }

    @Override
    public String describe() { return "WindowProcessor(" + duration + ")"; }
}
