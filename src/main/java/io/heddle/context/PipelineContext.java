package io.heddle.context;

import io.heddle.config.PipelineOptions;
import io.heddle.error.HeddleException;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Shared atomic signaling hub for a single pipeline run.
 *
 * <p>Ordering invariant: CAS the terminal state FIRST, then interrupt. Any thread
 * that wakes on an interrupt re-reads the atomic and gets a consistent verdict.
 * {@code Thread.interrupted()} is never used as state — clearing the flag would
 * mislabel a failure as a cancellation.
 *
 * <p>{@link #CURRENT} is a {@link ScopedValue} bound by {@code HeddleCore} on the
 * supervisor thread before any stage subtasks are forked. All stage virtual threads
 * inherit the binding automatically via structured concurrency's parent-child scope
 * semantics. User-defined stage code can call {@link #current()} from anywhere in
 * the call tree without needing the context threaded through constructors.
 */
public final class PipelineContext {

    /**
     * Scoped binding of the active {@code PipelineContext} for the current thread
     * and all its structured-concurrency children.
     */
    public static final ScopedValue<PipelineContext> CURRENT = ScopedValue.newInstance();

    /**
     * Scoped binding of the active source-stop callback. When bound, calling
     * {@code SOURCE_STOP.get().run()} signals the source stage to stop producing
     * after the current item. Bound by {@code HeddleCore.buildSupervisor} alongside
     * {@link #CURRENT}; unbound for pipelines without a managed source stage.
     */
    public static final ScopedValue<Runnable> SOURCE_STOP = ScopedValue.newInstance();

    /**
     * Scoped binding of the pipeline-level deadline as an absolute {@link Instant}.
     * Bound by {@code HeddleCore.buildSupervisor} when {@code PipelineOptions.deadline()}
     * is non-null. Stage code can call {@link #deadline()} to obtain the value without
     * constructor injection, enabling per-item timeout logic based on remaining wall time.
     */
    public static final ScopedValue<Instant> DEADLINE = ScopedValue.newInstance();

    /**
     * Returns the {@code PipelineContext} bound to the current thread's scope.
     *
     * @throws IllegalStateException if called outside a running pipeline stage
     */
    public static PipelineContext current() {
        if (!CURRENT.isBound()) {
            throw new IllegalStateException(
                    "PipelineContext.current() called outside an active pipeline stage. " +
                    "Ensure this method is only called from within Stage.process() or Stage.flush().");
        }
        return CURRENT.get();
    }

    /**
     * Returns the pipeline-level deadline if one was configured via
     * {@link io.heddle.config.PipelineOptions#deadline()}, or {@link Optional#empty()}
     * if no deadline is set or this is called outside a pipeline stage.
     */
    public static Optional<Instant> deadline() {
        return DEADLINE.isBound() ? Optional.of(DEADLINE.get()) : Optional.empty();
    }

    private static final TerminalState RUNNING_SENTINEL = new TerminalState.Running();

    private final PipelineOptions options;
    private final ConcurrentHashMap<Thread, String> stageThreads = new ConcurrentHashMap<>();
    private final AtomicReference<TerminalState> state = new AtomicReference<>(RUNNING_SENTINEL);

    public PipelineContext() {
        this.options = PipelineOptions.defaults();
    }

    public PipelineContext(PipelineOptions options) {
        this.options = options != null ? options : PipelineOptions.defaults();
    }

    /** Returns the options that were active when this pipeline was assembled. */
    public PipelineOptions options() { return options; }

    

    /**
     * Pre-register a stage thread before it starts running. This lets
     * {@link #interruptAll()} reach threads that haven't yet called
     * {@link #registerStage} from within their run loop (I8).
     */
    /**
     * Pre-register a stage thread before it starts running. This lets
     * {@link #interruptAll()} reach threads that haven't yet called
     * {@link #registerStage} from within their run loop.
     *
     * <p>Thread is the map key (identity-based), so a later {@link #registerStage}
     * call for the same thread simply updates the display name without creating a
     * duplicate entry. {@link #interruptAll()} therefore fires exactly once per thread.
     */
    public void preRegister(String stageId, Thread thread) {
        stageThreads.put(thread, stageId);
    }

    /** Called from inside the stage thread to confirm it is live. Idempotent. */
    public void registerStage(String stageId, Thread thread) {
        stageThreads.put(thread, stageId);
    }

    

    public void signalFailure(String stageId, Throwable cause) {
        signalFailure(stageId, cause, -1L);
    }

    public void signalFailure(String stageId, Throwable cause, long itemIndex) {
        if (state.compareAndSet(RUNNING_SENTINEL,
                new TerminalState.Failed(stageId, cause, System.nanoTime(), itemIndex))) {
            interruptAll();
        }
    }

    public void signalCompletion() {
        state.compareAndSet(RUNNING_SENTINEL, new TerminalState.Completed());
    }

    public void signalCancellation() {
        if (state.compareAndSet(RUNNING_SENTINEL, new TerminalState.Cancelled())) {
            interruptAll();
        }
    }

    private void interruptAll() {
        stageThreads.keySet().forEach(Thread::interrupt);
    }

    

    public TerminalState state()    { return state.get(); }
    public boolean isRunning()      { return state.get() instanceof TerminalState.Running; }

    /**
     * Called by channels before and after a blocking operation. Reads only the
     * atomic — never clears the interrupt flag.
     */
    public void checkSignal() {
        switch (state.get()) {
            case TerminalState.Failed f ->
                throw new HeddleException("pipeline aborted by stage '" + f.stageId() + "'", f.cause());
            case TerminalState.Cancelled _ ->
                throw new HeddleException("pipeline cancelled");
            case TerminalState.Running _ -> {}
            case TerminalState.Completed _ -> {}
        }
    }
}
