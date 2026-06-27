package io.heddle.internal;

import io.heddle.api.NChannel;
import io.heddle.context.PipelineContext;

/**
 * Deferred source descriptor. Channels and the shared {@link PipelineContext}
 * are allocated by {@link io.heddle.HeddleCore} before this is invoked, so the
 * source can be wired into them at assembly time.
 */
@FunctionalInterface
public interface SourceDescriptor<T> {
    @SuppressWarnings("rawtypes")
    Runnable build(String stageId, NChannel downstream, PipelineContext context);
}
