package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.Stage;
import io.heddle.api.StageContext;

/**
 * Identity pass-through stage whose sole purpose is to act as a marker that the
 * pipeline builder reads when sizing the downstream channel.
 *
 * <p>The {@link io.heddle.Pipeline#buffer(int)} operator inserts a
 * {@code BufferProcessor} into the stage list with the requested capacity. When
 * {@link io.heddle.HeddleCore} assembles the channel graph it reads the capacity
 * from the stage descriptor and creates the downstream {@link io.heddle.channel.HeddleChannel}
 * at that size. No data transformation occurs.
 */
public final class BufferProcessor<T> implements Stage<T, T>, Describable {

    private final int capacity;

    public BufferProcessor(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be positive");
        this.capacity = capacity;
    }

    public int capacity() { return capacity; }

    @Override
    public void process(Owned<T> item, StageContext<T> ctx) {
        ctx.emit(item.consume());
    }

    @Override
    public String describe() { return "BufferProcessor(" + capacity + ")"; }
}
