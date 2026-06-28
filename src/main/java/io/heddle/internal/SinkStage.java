package io.heddle.internal;

import io.heddle.api.NChannel;
import io.heddle.api.Sink;
import io.heddle.context.PipelineContext;
import io.heddle.context.TerminalState;
import io.heddle.error.HeddleException;

import java.util.concurrent.atomic.LongAdder;

/**
 * Egress stage. Consumes {@link Transfer} tokens from the last pipeline channel
 * and delivers each value to the user-supplied {@link Sink}, then calls
 * {@link Sink#onComplete()} or {@link Sink#onError(Throwable)} once the stream ends.
 *
 * <p>This stage drives the {@link Sink} lifecycle that {@link io.heddle.internal.PipelineStage}
 * cannot (PipelineStage only drives {@link io.heddle.api.Stage#process} and
 * {@link io.heddle.api.Stage#flush}). Without this adapter {@code FileSink} would
 * never flush/close its writer (S2).
 *
 * <p>{@link Transfer.Signal} tokens are silently dropped at the sink boundary.
 * The sink is the terminal consumer and has no downstream to forward signals to;
 * if user code needs to react to signals at the sink level, implement a custom
 * {@link Sink} and wrap it behind a signal-intercepting {@link io.heddle.api.Stage}.
 */
public final class SinkStage<T> implements Runnable {

    private final String stageId;
    private final NChannel<Transfer<T>> upstream;
    private final Sink<T> sink;
    private final PipelineContext context;
    private final LongAdder itemsProcessed = new LongAdder();

    public LongAdder itemsProcessed() { return itemsProcessed; }

    public SinkStage(
            String stageId,
            NChannel<Transfer<T>> upstream,
            Sink<T> sink,
            PipelineContext context) {
        this.stageId = stageId;
        this.upstream = upstream;
        this.sink     = sink;
        this.context  = context;
    }

    @Override
    public void run() {
        context.registerStage(stageId, Thread.currentThread());
        boolean failed = false;
        try {
            loop: while (context.isRunning()) {
                Transfer<T> token = upstream.take();
                switch (token) {
                    case Transfer.Complete<?> _ -> {
                        if (!context.isRunning()) failed = true;
                        break loop;
                    }
                    case Transfer.Signal<?, ?> _ -> { /* signals are not forwarded to Sink */ }
                    case Transfer.Ready<?> r -> {
                        @SuppressWarnings("unchecked")
                        T value = (T) r.value();
                        sink.accept(value);
                        itemsProcessed.increment();
                    }
                }
            }
        } catch (HeddleException victim) {
            failed = true;
        } catch (Throwable userEx) {
            failed = true;
            context.signalFailure(stageId, userEx);
        } finally {
            boolean wasInterrupted = Thread.interrupted();
            if (failed) {
                TerminalState s = context.state();
                Throwable cause = s instanceof TerminalState.Failed f ? f.cause() : null;
                try {
                    sink.onError(cause != null ? cause : new HeddleException("pipeline failed"));
                } catch (Throwable sinkEx) {
                    context.signalFailure(stageId, sinkEx);
                }
            } else {
                try {
                    sink.onComplete();
                } catch (Throwable sinkEx) {
                    context.signalFailure(stageId, sinkEx);
                }
            }
            if (wasInterrupted) Thread.currentThread().interrupt();
        }
    }
}
