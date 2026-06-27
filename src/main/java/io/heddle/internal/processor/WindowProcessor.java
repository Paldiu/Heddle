package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.StageContext;
import io.heddle.internal.transformer.StatefulTransformer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Collects items into time-bounded windows of type {@code List<T>}. A companion
 * virtual thread signals a flush every {@code duration}; the stage thread checks
 * the signal before processing each item and drains the current buffer when set.
 *
 * <p>The timer thread is started lazily on the first item and interrupted when
 * upstream completes. A partial window accumulated after the last timer tick is
 * flushed by {@link #flush}.
 *
 * <p>Unlike {@code batch(n)}, windows are time-driven rather than count-driven:
 * a window closes after {@code duration} regardless of how many items it holds.
 * An empty window (no items arrived during {@code duration}) produces no output.
 */
public final class WindowProcessor<T> extends StatefulTransformer<T, List<T>> implements Describable {

    private final Duration        duration;
    private final AtomicBoolean   flushFlag  = new AtomicBoolean(false);
    private final List<T>         buffer     = new ArrayList<>();
    private volatile Thread       timerThread;

    public WindowProcessor(Duration duration) {
        if (duration == null || duration.isNegative() || duration.isZero())
            throw new IllegalArgumentException("duration must be a positive Duration");
        this.duration = duration;
    }

    @Override
    public void process(Owned<T> item, StageContext<List<T>> ctx) {
        if (timerThread == null) {
            timerThread = Thread.ofVirtual().start(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        Thread.sleep(duration);
                        flushFlag.set(true);
                    }
                } catch (InterruptedException ignored) {}
            });
        }
        if (flushFlag.compareAndSet(true, false) && !buffer.isEmpty()) {
            ctx.emit(new ArrayList<>(buffer));
            buffer.clear();
        }
        buffer.add(item.consume());
    }

    @Override
    public void flush(StageContext<List<T>> ctx) {
        if (timerThread != null) {
            timerThread.interrupt();
            timerThread = null;
        }
        if (!buffer.isEmpty()) {
            ctx.emit(new ArrayList<>(buffer));
            buffer.clear();
        }
    }

    @Override
    public void reset() {
        if (timerThread != null) {
            timerThread.interrupt();
            timerThread = null;
        }
        flushFlag.set(false);
        buffer.clear();
    }

    @Override
    public String describe() { return "WindowProcessor(" + duration + ")"; }
}
