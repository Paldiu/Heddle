package io.heddle.internal;

/**
 * In-band data-path token. Three sealed variants cover the full data-path vocabulary:
 *
 * <ul>
 *   <li>{@link Ready} - a single live item flowing downstream.</li>
 *   <li>{@link Complete} - the end-of-stream sentinel that drives graceful drain.</li>
 *   <li>{@link Signal} - a typed control message with payload {@code U}, independent
 *       of the stream's item type {@code T}. Use for watermarks, flush triggers,
 *       schema-change notifications, or any in-band control plane message.</li>
 * </ul>
 *
 * <p>There is no {@code Failed} variant; failure is exclusively out-of-band via
 * {@link io.heddle.context.PipelineContext#signalFailure}. Routing a failed token
 * down the data path would trap it behind a backed-up buffer, reintroducing the
 * queue drag the out-of-band layer exists to eliminate.
 *
 * <p>{@code Complete} carries no payload and is safe to share as a singleton -
 * use {@link #complete()} to avoid allocating a new instance per end-of-stream.
 * {@code Signal} and {@code Ready} must never be singletons; their identity is
 * not load-bearing, but aliasing live values would break ownership semantics.
 */
public sealed interface Transfer<T> permits Transfer.Ready, Transfer.Complete, Transfer.Signal {

    record Ready<T>(T value) implements Transfer<T> {}

    record Complete<T>() implements Transfer<T> {}

    /**
     * In-band control message with typed payload {@code U}.
     *
     * <p>{@code T} is the stream's item type (a phantom needed to satisfy
     * {@code Transfer<T>}); {@code U} is the actual payload type and is
     * unrelated to {@code T}. Stages that do not recognise a signal's payload
     * type forward it unchanged via the default {@link io.heddle.api.Stage#onSignal}
     * implementation. Signals bypass the channel's backpressure drop policy so
     * that control messages are never silently discarded.
     */
    record Signal<T, U>(U payload) implements Transfer<T> {}

    @SuppressWarnings("unchecked")
    static <T> Complete<T> complete() {
        return (Complete<T>) Holder.INSTANCE;
    }

    static <T, U> Signal<T, U> signal(U payload) {
        return new Signal<>(payload);
    }

    final class Holder {
        private Holder() {}
        static final Complete<?> INSTANCE = new Complete<>();
    }
}
