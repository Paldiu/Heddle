package io.heddle.context;

/**
 * Atomic terminal state for a pipeline run. Exactly one of these is ever set;
 * once set it is never changed. {@link Running} is the initial state and the only
 * non-terminal one.
 *
 * <p>Always allocate a fresh {@code Running()} as the CAS sentinel stored in
 * {@link PipelineContext} - do not share instances across pipeline runs, since CAS
 * uses reference equality.
 */
public sealed interface TerminalState
        permits TerminalState.Running,
                TerminalState.Completed,
                TerminalState.Cancelled,
                TerminalState.Failed {

    record Running()   implements TerminalState {}

    record Completed() implements TerminalState {}

    record Cancelled() implements TerminalState {}

    record Failed(String stageId, Throwable cause, long atNanos, long itemIndex) implements TerminalState {}
}
