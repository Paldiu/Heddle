package io.heddle.internal;

/**
 * In-band data-path marker. {@code Ready} carries a live item; {@code Complete}
 * is the end-of-stream sentinel that drives graceful drain.
 *
 * <p>There is no {@code Failed} variant — failure is exclusively out-of-band via
 * {@link io.heddle.context.PipelineContext#signalFailure}. Routing a failed token
 * down the data path would trap it behind a backed-up buffer, reintroducing the
 * queue drag the out-of-band layer exists to eliminate.
 *
 * <p>{@code Complete} carries no payload and is safe to share as a singleton —
 * use {@link #complete()} to avoid allocating a new instance per end-of-stream.
 * Do NOT singleton {@code Ready}: its identity is never load-bearing, but aliasing
 * live values would break ownership semantics.
 */
public sealed interface Transfer<T> permits Transfer.Ready, Transfer.Complete {

    record Ready<T>(T value) implements Transfer<T> {}

    record Complete<T>() implements Transfer<T> {}

    @SuppressWarnings("unchecked")
    static <T> Complete<T> complete() {
        return (Complete<T>) Holder.INSTANCE;
    }

    final class Holder {
        private Holder() {}
        static final Complete<?> INSTANCE = new Complete<>();
    }
}
