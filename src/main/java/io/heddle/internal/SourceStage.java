package io.heddle.internal;

import io.heddle.api.Emitter;
import io.heddle.api.NChannel;
import io.heddle.context.PipelineContext;
import io.heddle.error.HeddleException;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Ingress stage. Pumps items from a producer into the first pipeline channel
 * and then emits {@link Transfer#complete()} to signal end-of-stream.
 *
 * <p>Three producer shapes are supported via static factories:
 * <ul>
 *   <li>{@link #fromIterable} - finite, pull-style.</li>
 *   <li>{@link #fromSupplier} - pull-style; returns {@code null} at EOS.</li>
 *   <li>{@link #fromEmitter}  - push-style; producer calls {@code emit()} directly.</li>
 * </ul>
 *
 * <p>On cancellation/failure the stage exits immediately; channels throw
 * {@link HeddleException} on every op once the context is terminal.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public final class SourceStage<T> implements Runnable {

    private final String stageId;
    private final NChannel<Transfer<T>> downstream;
    private final PipelineContext context;
    private final Consumer<Emitter<T>> producer;

    private volatile boolean stopRequested = false;

    /**
     * Signals this source to stop after the current item. Safe to call from any
     * thread; the write is visible to the source VT on its next emit().
     */
    public void stop() { stopRequested = true; }

    private SourceStage(
            String stageId,
            NChannel<Transfer<T>> downstream,
            PipelineContext context,
            Consumer<Emitter<T>> producer) {
        this.stageId    = stageId;
        this.downstream = downstream;
        this.context    = context;
        this.producer   = producer;
    }

    /** Iterable source. */
    public static <T> SourceStage<T> fromIterable(
            String stageId,
            Iterable<? extends T> source,
            NChannel downstream,
            PipelineContext context) {
        NChannel<Transfer<T>> typed = (NChannel<Transfer<T>>) downstream;
        return new SourceStage<>(stageId, typed, context,
                emitter -> { for (T item : source) emitter.emit(item); });
    }

    /** Supplier source; {@code null} return signals end-of-stream. */
    public static <T> SourceStage<T> fromSupplier(
            String stageId,
            Supplier<? extends T> source,
            NChannel downstream,
            PipelineContext context) {
        NChannel<Transfer<T>> typed = (NChannel<Transfer<T>>) downstream;
        return new SourceStage<>(stageId, typed, context, emitter -> {
            T item;
            while ((item = source.get()) != null) emitter.emit(item);
        });
    }

    /** Push-style emitter source. */
    public static <T> SourceStage<T> fromEmitter(
            String stageId,
            Consumer<? super Emitter<T>> producer,
            NChannel downstream,
            PipelineContext context) {
        NChannel<Transfer<T>> typed = (NChannel<Transfer<T>>) downstream;
        return new SourceStage<>(stageId, typed, context,
                emitter -> ((Consumer<Emitter<T>>) producer).accept(emitter));
    }

    @Override
    public void run() {
        context.registerStage(stageId, Thread.currentThread());
        try {
            Emitter<T> emitter = item -> {
                if (stopRequested) throw new SourceStopException();
                downstream.put(new Transfer.Ready<>(item));
            };
            producer.accept(emitter);
        } 
        catch (SourceStopException ignored) {} 
        catch (HeddleException ignored) {} 
        catch (Throwable t) {
            context.signalFailure(stageId, t);
        } finally {
            Thread.interrupted(); 
            try {
                downstream.put(Transfer.complete());
            } catch (RuntimeException ignored) {}
        }
    }
}
