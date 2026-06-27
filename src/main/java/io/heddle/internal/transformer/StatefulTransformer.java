package io.heddle.internal.transformer;

import io.heddle.api.Owned;
import io.heddle.api.Stage;
import io.heddle.api.StageContext;
import io.heddle.internal.processor.*;

/**
 * Base class for stages that accumulate state across multiple items
 * (e.g. {@code batch}, {@code window}, {@code dedupe}).
 *
 * <p>Subclasses must override {@link #process} to accumulate state and
 * {@link #flush} to emit any buffered items when upstream completes.
 * The runtime calls {@code flush} exactly once, before the {@code Complete}
 * token propagates to the downstream channel.
 */
public sealed abstract class StatefulTransformer<I, O> implements Stage<I, O> 
                permits BatchProcessor, BackpressurePolicyProcessor, DistinctProcessor, 
                ThrottleProcessor, TimeoutProcessor, WindowProcessor {

    @Override
    public abstract void process(Owned<I> item, StageContext<O> ctx);

    @Override
    public abstract void flush(StageContext<O> ctx);

    /** Reset internal state. Called by the runtime before a pipeline restart (if supported). */
    public void reset() {}
}
