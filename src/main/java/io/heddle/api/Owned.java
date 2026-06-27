package io.heddle.api;

/**
 * Thread-confined, consume-once wrapper for pipeline items.
 *
 * <p>The pipeline runtime creates an {@code Owned} wrapper for each item as it passes
 * between stages. The wrapper is created in, and may only be consumed by, the virtual
 * thread that owns it. {@link #consume()} may be called exactly once; subsequent calls
 * throw {@link OwnershipViolationException}.
 *
 * <p>This ownership model enforces a correctness discipline: a stage that accidentally
 * retains a reference to an item across invocations is caught immediately rather than
 * silently producing corrupted output. It is not a memory-safety or cryptographic
 * security guarantee.
 *
 * <p>The channel put/take that delivers the item to this thread establishes a
 * happens-before edge, so fields of the wrapped value do not require additional volatile
 * synchronization when accessed solely by the consuming thread.
 *
 * <p>For payloads containing sensitive material such as {@code char[]}, {@code byte[]},
 * or key objects, supply a {@link io.heddle.security.ClearHook} so the runtime can zero
 * backing buffers after the item is consumed.
 *
 * @param <T> the type of the owned item
 * @see io.heddle.security.ClearHook
 * @see Stage
 */
public final class Owned<T> {

    private T value;
    private boolean consumed;
    private final long ownerThreadId;

    /**
     * Creates a new {@code Owned} wrapper for the given value, bound to the calling
     * thread.
     *
     * @param value the item to wrap
     */
    public Owned(T value) {
        this.value = value;
        this.ownerThreadId = Thread.currentThread().threadId();
    }

    /**
     * Retrieves the wrapped item and marks this wrapper as consumed.
     *
     * <p>This method must be called from the same virtual thread that created this
     * wrapper. It may only be called once; a second call throws
     * {@link OwnershipViolationException}.
     *
     * @return the wrapped item
     * @throws OwnershipViolationException if called from a thread other than the owner
     *                                     thread, or if the item has already been
     *                                     consumed
     */
    public T consume() {
        enforceOwnership();
        if (consumed) throw new OwnershipViolationException("item already consumed");
        T ref = this.value;
        this.value = null;
        this.consumed = true;
        return ref;
    }

    /**
     * Returns {@code true} if this wrapper has already been consumed via
     * {@link #consume()}.
     *
     * @return {@code true} after {@link #consume()} has been called successfully;
     *         {@code false} otherwise
     */
    public boolean isConsumed() { return consumed; }

    private void enforceOwnership() {
        long current = Thread.currentThread().threadId();
        if (current != ownerThreadId) {
            throw new OwnershipViolationException(
                "thread " + current + " accessed item owned by thread " + ownerThreadId);
        }
    }

    /**
     * Thrown when an {@link Owned} wrapper is accessed from a thread other than the
     * thread that created it, or when a wrapper that has already been consumed is
     * accessed a second time.
     */
    public static final class OwnershipViolationException extends RuntimeException {

        /**
         * Constructs an {@code OwnershipViolationException} with the given detail
         * message.
         *
         * @param msg the detail message
         */
        public OwnershipViolationException(String msg) { super(msg); }
    }
}
