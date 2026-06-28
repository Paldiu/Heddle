package io.heddle.internal.handoff;

import io.heddle.api.Owned;

/**
 * Formalises the atomic handoff seal between two pipeline stages.
 *
 * <p>In Heddle, ownership transfers through the channel: {@code ArrayBlockingQueue.take()}
 * atomically removes exactly one element, so exactly one consumer ever acquires a given
 * item. This class makes that protocol explicit, {@link #take(Object)} is the single
 * site where a raw value becomes an {@link Owned} token.
 *
 * <p>The happens-before guarantee is provided by the queue's {@code put/take}
 * pair, not by any field in {@code Owned}. No {@code volatile} is needed.
 */
public final class LinearHandoff {

    private LinearHandoff() {}

    /**
     * Wrap a value that was just taken from a channel in an {@link Owned} token,
     * binding it to the calling thread as the sole legal consumer.
     */
    public static <T> Owned<T> take(T value) {
        return new Owned<>(value);
    }
}
