package io.heddle.config;

/**
 * Controls error-propagation behaviour when one upstream pipeline in a
 * {@link io.heddle.Heddle#merge(io.heddle.Pipeline[])} call fails while others are
 * still running.
 *
 * <p>Configure on the merged pipeline via
 * {@link PipelineOptions#withMergeStrategy(MergeStrategy)}:
 *
 * <pre>{@code
 * Heddle.merge(p1, p2, p3)
 *       .withOptions(PipelineOptions.defaults()
 *               .withMergeStrategy(MergeStrategy.BEST_EFFORT))
 *       .forEach(System.out::println);
 * }</pre>
 *
 * @see PipelineOptions#withMergeStrategy(MergeStrategy)
 * @see io.heddle.Heddle#merge(io.heddle.Pipeline[])
 */
public enum MergeStrategy {

    /**
     * Stop the merged output as soon as any upstream fails.
     *
     * <p>Remaining upstreams are allowed to run to natural completion, but their
     * output is discarded once the first failure is detected. The first upstream error
     * is rethrown as the merged pipeline's failure cause. This is the default strategy.
     */
    FAIL_FAST,

    /**
     * Keep draining healthy upstreams after one fails.
     *
     * <p>All upstreams run to completion regardless of individual failures. Once every
     * upstream has finished, all collected errors are surfaced together as suppressed
     * exceptions on a single {@link RuntimeException} thrown by the merged pipeline.
     */
    BEST_EFFORT
}
