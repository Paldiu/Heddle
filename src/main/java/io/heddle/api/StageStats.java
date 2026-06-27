package io.heddle.api;

/**
 * Point-in-time snapshot of a single pipeline stage's channel metrics.
 *
 * <p>Instances are returned as elements of {@link PipelineStats#stages()} from a call
 * to {@link PipelineHandle#stats()}. All values reflect the state of the channel at
 * the moment the snapshot was taken and may be stale by the time they are read.
 *
 * @param name         the stage identifier, for example {@code "heddle-stage-2"}
 * @param channelSize  the number of items currently queued in this stage's output channel
 * @param droppedCount the cumulative number of items dropped by this stage's output
 *                     channel; non-zero only when the configured
 *                     {@link io.heddle.policies.BackpressurePolicy} is
 *                     {@link io.heddle.policies.BackpressurePolicy#DROP}
 * @see PipelineHandle#stats()
 * @see PipelineStats
 */
public record StageStats(String name, int channelSize, long droppedCount) {}
