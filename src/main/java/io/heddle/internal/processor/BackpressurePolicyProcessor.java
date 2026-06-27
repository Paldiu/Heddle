package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.StageContext;
import io.heddle.internal.transformer.StatefulTransformer;
import io.heddle.policies.BackpressurePolicy;

/**
 * Zero-copy marker stage that annotates the downstream channel with a
 * {@link BackpressurePolicy}. Items pass through unchanged; {@code HeddleCore}
 * reads the policy during channel allocation and replaces the default
 * {@link BackpressurePolicy#BLOCK} channel with one using the specified policy.
 *
 * <p>Usage:
 * <pre>{@code
 *   Heddle.generate(fastSource)
 *       .map(transform)
 *       .backpressure(BackpressurePolicy.DROP)
 *       .batch(100)
 *       .sink(sink)
 *       .start();
 * }</pre>
 * The {@code DROP} policy is applied to the channel between {@code backpressure()}
 * and {@code batch(100)}.
 */
public final class BackpressurePolicyProcessor<T> extends StatefulTransformer<T, T> implements Describable {

    private final BackpressurePolicy policy;

    public BackpressurePolicyProcessor(BackpressurePolicy policy) {
        if (policy == null) throw new NullPointerException("policy must not be null");
        this.policy = policy;
    }

    public BackpressurePolicy policy() { return policy; }

    @Override
    public void process(Owned<T> item, StageContext<T> ctx) {
        ctx.emit(item.consume());
    }

    @Override
    public void flush(StageContext<T> ctx) {}

    @Override
    public String describe() {
        return "BackpressurePolicyProcessor(" + policy.getClass().getSimpleName() + ")";
    }
}
