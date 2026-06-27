package io.heddle.internal;

import io.heddle.api.DeadLetterSink;
import io.heddle.api.NChannel;
import io.heddle.api.Owned;
import io.heddle.api.Stage;
import io.heddle.api.StageContext;
import io.heddle.api.TransactionalStage;
import io.heddle.concurrent.AdmissionController;
import io.heddle.context.PipelineContext;
import io.heddle.error.ErrorStrategy;
import io.heddle.error.HeddleException;

/**
 * Core execution loop for a single virtual thread in the pipeline.
 *
 * <p>Originator vs. victim distinction:
 * <ul>
 *   <li>If user code throws, this stage is the <em>originator</em>: it applies the
 *       configured {@link ErrorStrategy} — stopping the pipeline, skipping the item,
 *       retrying, or routing to a dead-letter sink.</li>
 *   <li>If {@link HeddleException} arrives from a channel operation, another stage
 *       already signaled — this stage is a <em>victim</em> and just unwinds.</li>
 * </ul>
 *
 * <p>An optional {@link AdmissionController} may be supplied to cap the number of
 * items actively inside {@code Stage.process()} at any moment. The permit is
 * acquired before each {@code process()} call and released in a {@code finally}
 * block, so it is always returned even if user code throws.
 *
 * <p>The outer {@code finally} block always calls {@link #forwardCompletion()} so
 * the downstream stage never hangs waiting for a token that will never arrive.
 */
public final class PipelineStage<I, O> implements Runnable {

    private final String stageId;
    private final NChannel<Transfer<I>> upstream;
    private final NChannel<Transfer<O>> downstream;
    private final Stage<I, O> userStage;
    private final PipelineContext context;
    private final AdmissionController admissionController;
    private final ErrorStrategy strategy;

    public PipelineStage(
            String stageId,
            NChannel<Transfer<I>> upstream,
            NChannel<Transfer<O>> downstream,
            Stage<I, O> userStage,
            PipelineContext context) {
        this(stageId, upstream, downstream, userStage, context, null, new ErrorStrategy.Stop());
    }

    public PipelineStage(
            String stageId,
            NChannel<Transfer<I>> upstream,
            NChannel<Transfer<O>> downstream,
            Stage<I, O> userStage,
            PipelineContext context,
            AdmissionController admissionController) {
        this(stageId, upstream, downstream, userStage, context, admissionController, new ErrorStrategy.Stop());
    }

    public PipelineStage(
            String stageId,
            NChannel<Transfer<I>> upstream,
            NChannel<Transfer<O>> downstream,
            Stage<I, O> userStage,
            PipelineContext context,
            AdmissionController admissionController,
            ErrorStrategy strategy) {
        this.stageId             = stageId;
        this.upstream            = upstream;
        this.downstream          = downstream;
        this.userStage           = userStage;
        this.context             = context;
        this.admissionController = admissionController;
        this.strategy            = strategy;
    }

    @Override
    public void run() {
        context.registerStage(stageId, Thread.currentThread());

        StageContext<O> stageCtx = value -> downstream.put(new Transfer.Ready<>(value));
        long itemIndex = 0;

        try {
            loop: while (context.isRunning()) {
                Transfer<I> transfer = upstream.take();

                switch (transfer) {
                    case Transfer.Complete<?> _ -> {
                        userStage.flush(stageCtx);
                        break loop;
                    }
                    case Transfer.Ready<?> ready -> {
                        @SuppressWarnings("unchecked")
                        I value = (I) ready.value();
                        if (admissionController != null) admissionController.acquire();
                        try {
                            int totalAttempts = (strategy instanceof ErrorStrategy.Retry r) ? r.max() + 1 : 1;
                            Throwable caught = null;
                            for (int attempt = 0; attempt < totalAttempts; attempt++) {
                                try {
                                    userStage.process(new Owned<>(value), stageCtx);
                                    caught = null;
                                    break;
                                } catch (HeddleException he) {
                                    throw he;
                                } catch (Throwable ex) {
                                    caught = ex;
                                }
                            }
                            if (caught != null) {
                                if (userStage instanceof TransactionalStage tx) {
                                    try { tx.rollback(caught); } catch (Throwable ignored) {}
                                }
                                switch (strategy) {
                                    case ErrorStrategy.Stop _ -> {
                                        context.signalFailure(stageId, caught, itemIndex);
                                        return;
                                    }
                                    case ErrorStrategy.Skip _ -> { /* drop item and continue */ }
                                    case ErrorStrategy.Retry _ -> {
                                        context.signalFailure(stageId, caught, itemIndex);
                                        return;
                                    }
                                    case ErrorStrategy.DeadLetter dl -> {
                                        @SuppressWarnings("unchecked")
                                        DeadLetterSink<I> sink = (DeadLetterSink<I>) dl.sink();
                                        sink.accept(value, caught);
                                    }
                                }
                            }
                            itemIndex++;
                        } finally {
                            if (admissionController != null) admissionController.release();
                        }
                    }
                }
            }
        } 
        catch (HeddleException ignored) {} // Victim of pipeline termination; upstream already signaled
        catch (Throwable fatal) {
            context.signalFailure(stageId, fatal); // Internal Heddle error
        } 
        finally {
            Thread.interrupted(); 
            forwardCompletion();
        }
    }

    private void forwardCompletion() {
        try {
            downstream.put(Transfer.complete());
        } catch (RuntimeException ignored) {
            
        }
    }
}
