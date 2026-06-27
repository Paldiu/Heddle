package io.heddle.security;

/**
 * Callback that overwrites the sensitive contents of a pipeline payload before the
 * runtime releases its reference.
 *
 * <p>The generic pipeline ({@link io.heddle.api.Owned} wrappers and internal transfer
 * types) can only null a reference when it is done with an item; it cannot zero the
 * backing memory. For payloads that hold sensitive material such as {@code char[]},
 * {@code byte[]}, or key objects, supply a {@code ClearHook} that overwrites backing
 * buffers directly, reducing the window during which secret material is present in the
 * heap.
 *
 * <p>{@code ClearHook} is a functional interface and is typically implemented as a
 * method reference or lambda:
 *
 * <pre>{@code
 * ClearHook<char[]> wipePassword = buf -> Arrays.fill(buf, '\0');
 * ClearHook<byte[]> wipeKey      = buf -> Arrays.fill(buf, (byte) 0);
 * }</pre>
 *
 * <p><b>Security note:</b> Clearing is best-effort. The JVM may have duplicated backing
 * buffers during garbage collection or JIT compilation. Do not rely on this interface
 * as a cryptographic guarantee of timely secret erasure; document its use as a
 * risk-reduction measure rather than a security boundary.
 *
 * @param <T> the payload type whose backing memory this hook zeroes
 * @see SensitiveDataHandler
 */
@FunctionalInterface
public interface ClearHook<T> {

    /**
     * Overwrites or invalidates the sensitive contents of the given value.
     *
     * <p>Implementations should zero all backing buffers within {@code value}. This
     * method is called by {@link SensitiveDataHandler#clear(Object)} at the point when
     * the pipeline runtime is finished with the payload. Any exception thrown by this
     * method is silently suppressed by the handler.
     *
     * @param value the payload whose sensitive contents should be zeroed; never
     *              {@code null} when called by {@link SensitiveDataHandler}
     */
    void clear(T value);
}
