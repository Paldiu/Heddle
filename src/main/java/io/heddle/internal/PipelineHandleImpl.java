package io.heddle.internal;

import io.heddle.api.PipelineHandle;
import io.heddle.api.PipelineStats;
import io.heddle.api.StageStats;
import io.heddle.channel.HeddleChannel;
import io.heddle.context.PipelineContext;
import io.heddle.context.TerminalState;
import io.heddle.util.ThreadUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Concrete {@link PipelineHandle} produced by {@link io.heddle.HeddleCore}.
 *
 * <p>A single <em>supervisor</em> virtual thread is started on {@link #start()}.
 * The supervisor owns the scope that contains all stage VTs; callers join only the
 * supervisor, not N individual threads. This simplifies lifecycle management and
 * ensures the supervisor cannot exit before every stage has finished.
 *
 * <p>{@link #start()} is guarded by an {@link AtomicBoolean} CAS so concurrent
 * callers are safely rejected without a data race on the thread reference.
 */
public final class PipelineHandleImpl implements PipelineHandle {

    private final Runnable supervisorTask;
    private final PipelineContext context;
    private final String namePrefix;
    private final Runnable sourceStop;
    private final List<HeddleChannel<?>> channels;
    private final List<String> stageNames;
    private final LongAdder itemsProcessed;
    private final List<PipelineHandle> companions;

    private final AtomicBoolean started = new AtomicBoolean();
    private volatile Thread supervisor;
    private volatile Instant startInstant;
    private volatile Runnable onComplete;
    private volatile Consumer<Throwable> onError;

    public PipelineHandleImpl(
            Runnable supervisorTask,
            PipelineContext context,
            String namePrefix,
            Runnable sourceStop,
            List<HeddleChannel<?>> channels,
            List<String> stageNames,
            LongAdder itemsProcessed,
            List<PipelineHandle> companions) {
        this.supervisorTask  = supervisorTask;
        this.context         = context;
        this.namePrefix      = namePrefix;
        this.sourceStop      = sourceStop;
        this.channels        = channels;
        this.stageNames      = stageNames;
        this.itemsProcessed  = itemsProcessed;
        this.companions      = companions;
    }

    public PipelineHandleImpl(
            Runnable supervisorTask,
            PipelineContext context,
            String namePrefix,
            Runnable sourceStop) {
        this(supervisorTask, context, namePrefix, sourceStop,
             Collections.emptyList(), Collections.emptyList(), new LongAdder(),
             Collections.emptyList());
    }

    @Override
    public PipelineHandle start() {
        if (!started.compareAndSet(false, true))
            throw new IllegalStateException("pipeline already started");
        startInstant = Instant.now();
        for (PipelineHandle companion : companions) {
            companion.start();
        }
        supervisor = Thread.ofVirtual()
                .name(namePrefix + "-supervisor")
                .start(supervisorTask);
        return this;
    }

    @Override
    public void stop() {
        sourceStop.run();
    }

    @Override
    public void cancel() {
        context.signalCancellation();
        for (PipelineHandle companion : companions) {
            companion.cancel();
        }
    }

    @Override
    public void awaitCompletion() {
        if (supervisor == null) throw new IllegalStateException("pipeline not started");
        ThreadUtils.joinAll(supervisor);
        fireCallbacks();
    }

    @Override
    public void awaitCompletion(Duration timeout) throws TimeoutException {
        if (supervisor == null) throw new IllegalStateException("pipeline not started");
        if (timeout == null)    throw new NullPointerException("timeout must not be null");

        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        boolean interrupted = false;

        while (supervisor.isAlive()) {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                context.signalCancellation();
                if (interrupted) Thread.currentThread().interrupt();
                throw new TimeoutException("pipeline did not complete within " + timeout);
            }
            try {
                supervisor.join(TimeUnit.NANOSECONDS.toMillis(remaining) + 1);
            } catch (InterruptedException e) {
                interrupted = true;
                Thread.interrupted();
            }
        }

        if (interrupted) Thread.currentThread().interrupt();
        fireCallbacks();
    }

    @Override
    public boolean isRunning()   { return context.isRunning(); }

    @Override
    public boolean isComplete()  { return !context.isRunning(); }

    @Override
    public boolean isCancelled() { return context.state() instanceof TerminalState.Cancelled; }

    @Override
    public boolean isFailed()    { return context.state() instanceof TerminalState.Failed; }

    @Override
    public Optional<Throwable> failureCause() {
        TerminalState s = context.state();
        if (s instanceof TerminalState.Failed f) return Optional.of(f.cause());
        return Optional.empty();
    }

    @Override
    public PipelineHandle onComplete(Runnable callback) {
        this.onComplete = callback;
        return this;
    }

    @Override
    public PipelineHandle onError(Consumer<Throwable> callback) {
        this.onError = callback;
        return this;
    }

    @Override
    public PipelineStats stats() {
        List<StageStats> stageStatsList = new ArrayList<>(stageNames.size());
        for (int i = 0; i < stageNames.size(); i++) {
            HeddleChannel<?> ch = channels.get(i + 1);
            stageStatsList.add(new StageStats(stageNames.get(i), ch.size(), ch.droppedCount()));
        }
        Duration elapsed = startInstant != null
                ? Duration.between(startInstant, Instant.now())
                : Duration.ZERO;
        return new PipelineStats(
                Collections.unmodifiableList(stageStatsList),
                itemsProcessed.longValue(),
                elapsed,
                context.state());
    }

    private void fireCallbacks() {
        if (isFailed() && onError != null) {
            failureCause().ifPresent(onError);
        } else if (!isFailed() && !isCancelled() && onComplete != null) {
            onComplete.run();
        }
    }
}
