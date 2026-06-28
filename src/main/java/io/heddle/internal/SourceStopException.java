package io.heddle.internal;

/**
 * Thrown by the {@link SourceStage} emitter when a downstream stage signals that
 * the source should stop. Treated as normal completion, not a pipeline failure.
 * Stack trace is suppressed because this is control-flow, not an error.
 */
final class SourceStopException extends RuntimeException {
    SourceStopException() { super(null, null, true, false); }
}
