package io.heddle.api;

/**
 * Bounded, blocking communication channel between two adjacent pipeline stages.
 *
 * <p>{@code NChannel} is the backpressure boundary between a producer stage and its
 * downstream consumer stage. Both operations are blocking: {@link #put(Object)} parks
 * the caller when the channel is at capacity, and {@link #take()} parks the caller when
 * the channel is empty. Because all stage threads are virtual threads, both parks
 * release their carrier threads, incurring no platform-thread cost.
 *
 * <p>Implementations must be thread-safe and must support exactly one concurrent
 * producer thread and one concurrent consumer thread.
 *
 * @param <T> the type of items transferred through this channel
 * @see io.heddle.channel.HeddleChannel
 * @see io.heddle.policies.BackpressurePolicy
 */
public interface NChannel<T> {

    /**
     * Inserts the specified item into this channel, blocking if the channel is
     * currently at capacity.
     *
     * <p>The calling virtual thread parks until a slot becomes available, releasing
     * its carrier thread without blocking a platform thread.
     *
     * @param item the item to insert
     */
    void put(T item);

    /**
     * Retrieves and removes the head item of this channel, blocking until an item is
     * available if the channel is currently empty.
     *
     * <p>The calling virtual thread parks until an item is available, releasing its
     * carrier thread without blocking a platform thread.
     *
     * @return the head item
     */
    T take();
}
