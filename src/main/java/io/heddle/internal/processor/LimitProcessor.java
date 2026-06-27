package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.Stage;
import io.heddle.api.StageContext;
import io.heddle.context.PipelineContext;

/**
 * Passes at most {@code n} items downstream, then signals the source to stop
 * producing via {@link PipelineContext#SOURCE_STOP}. The source exits cleanly
 * after its current item rather than filling the channel indefinitely (M0).
 */
public final class LimitProcessor<T> implements Stage<T, T>, Describable {

    private final long n;
    private long emitted = 0;
    private boolean stopSignaled = false;
    private Runnable stopCallback;

    public LimitProcessor(long n) {
        if (n < 0) throw new IllegalArgumentException("n must be non-negative");
        this.n = n;
    }

    public void setStopCallback(Runnable callback) {
        this.stopCallback = callback;
    }

    @Override
    public void process(Owned<T> item, StageContext<T> ctx) {
        T value = item.consume();
        if (emitted < n) {
            ctx.emit(value);
            emitted++;
        }
        if (emitted >= n) {
            signalStop();
        }
    }

    @Override
    public void flush(StageContext<T> ctx) {
        signalStop();
    }

    private void signalStop() {
        if (!stopSignaled) {
            stopSignaled = true;
            if (stopCallback != null) {
                stopCallback.run();
            } else if (PipelineContext.SOURCE_STOP.isBound()) {
                PipelineContext.SOURCE_STOP.get().run();
            }
        }
    }

    @Override
    public String describe() { return "LimitProcessor(" + n + ")"; }
}
