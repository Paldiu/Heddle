package io.heddle.api;

import io.heddle.context.TerminalState;

import java.time.Duration;
import java.util.List;

/**
 * Point-in-time snapshot of a running or completed pipeline's metrics.
 *
 * <p>Returned by {@link PipelineHandle#stats()} and reflects the state of the pipeline
 * at the moment the call was made. All values may be stale by the time they are read.
 *
 * @param stages         per-stage channel snapshots in pipeline declaration order;
 *                       each element corresponds to one {@link Stage} appended to the
 *                       builder
 * @param itemsProcessed the total number of items successfully delivered to the
 *                       terminal {@link Sink} since {@link PipelineHandle#start()} was
 *                       called
 * @param elapsed        the wall-clock duration elapsed since {@link PipelineHandle#start()}
 *                       was called; zero if the pipeline has not yet started
 * @param state          the current terminal state; a {@code Running} instance indicates
 *                       the pipeline is still active
 * @see PipelineHandle#stats()
 * @see StageStats
 */
public record PipelineStats(
        List<StageStats> stages,
        long itemsProcessed,
        Duration elapsed,
        TerminalState state) {}
