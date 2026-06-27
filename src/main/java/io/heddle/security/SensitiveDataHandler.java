package io.heddle.security;

import java.util.function.Consumer;

/**
 * Coordinates zeroing of backing buffers for sensitive pipeline payloads.
 *
 * <p>Attach a {@code SensitiveDataHandler} to a pipeline stage by supplying it as the
 * clear hook in {@link io.heddle.channel.HeddleChannel#drainAndRelease}. When the
 * runtime is done with a payload, it calls {@link #clear(Object)} to invoke the
 * underlying {@link ClearHook}, overwriting any sensitive memory before the reference
 * is released to the garbage collector.
 *
 * <p><b>Security note:</b> Clearing is best-effort. The JVM may have duplicated backing
 * buffers during garbage collection or JIT compilation, and no mechanism in this class
 * prevents that. Do not rely on {@code SensitiveDataHandler} as a cryptographic
 * guarantee of timely secret erasure. Document its use as a risk-reduction measure,
 * not a security boundary.
 *
 * @param <T> the payload type whose backing memory is managed by this handler
 * @see ClearHook
 */
public final class SensitiveDataHandler<T> {

    private final ClearHook<T> hook;

    /**
     * Constructs a {@code SensitiveDataHandler} backed by the given {@link ClearHook}.
     *
     * @param hook the hook that overwrites the sensitive contents of a payload;
     *             may be {@code null}, in which case {@link #clear(Object)} is a no-op
     */
    public SensitiveDataHandler(ClearHook<T> hook) {
        this.hook = hook;
    }

    /**
     * Invokes the underlying {@link ClearHook} on the given value if both the hook and
     * the value are non-{@code null}.
     *
     * <p>Any exception thrown by the hook is silently suppressed so that a buggy or
     * partially-initialized hook never disrupts pipeline cleanup.
     *
     * @param value the payload to clear; if {@code null}, this method is a no-op
     */
    public void clear(T value) {
        if (hook != null && value != null) {
            try {
                hook.clear(value);
            } catch (Throwable ignored) { /* best-effort */ }
        }
    }

    /**
     * Returns a {@link Consumer} view of this handler that delegates each accepted
     * value to {@link #clear(Object)}.
     *
     * <p>Use this method to integrate the handler into contexts that accept a
     * {@code Consumer}, such as cleanup callbacks or stream operations.
     *
     * @return a {@code Consumer} that clears each value passed to it
     */
    public Consumer<T> asConsumer() {
        return this::clear;
    }
}
