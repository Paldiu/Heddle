/**
 * Heddle: a virtual-thread pipeline library for Java 21 and later.
 *
 * <p>Heddle provides a fluent builder API for composing data-processing pipelines that
 * run entirely on Project Loom virtual threads. Each pipeline stage executes on its own
 * virtual thread, connected to adjacent stages through bounded blocking channels. Blocking
 * on a channel releases the underlying carrier thread, so thousands of concurrent stages
 * cost no more platform threads than a handful.
 *
 * <h2>Public API packages</h2>
 * <ul>
 *   <li>{@code io.heddle} - primary entry points: {@link io.heddle.Heddle} (factory
 *       methods) and {@link io.heddle.Pipeline} (fluent builder)</li>
 *   <li>{@code io.heddle.api} - stage, sink, source, and handle contracts</li>
 *   <li>{@code io.heddle.config} - {@link io.heddle.config.PipelineOptions} and
 *       {@link io.heddle.config.MergeStrategy}</li>
 *   <li>{@code io.heddle.error} - exception types and
 *       {@link io.heddle.error.ErrorStrategy}</li>
 *   <li>{@code io.heddle.interop} - {@link io.heddle.interop.HeddleQueue} pipeline
 *       bridge</li>
 *   <li>{@code io.heddle.policies} - {@link io.heddle.policies.BackpressurePolicy}</li>
 *   <li>{@code io.heddle.security} - {@link io.heddle.security.SensitiveDataHandler}
 *       and {@link io.heddle.security.ClearHook}</li>
 *   <li>{@code io.heddle.transaction} - {@link io.heddle.transaction.TransactionStage}</li>
 * </ul>
 */
module io.heddle {
    exports io.heddle;
    exports io.heddle.api;
    exports io.heddle.buffer;
    exports io.heddle.config;
    exports io.heddle.error;
    exports io.heddle.interop;
    exports io.heddle.memory;
    exports io.heddle.policies;
    exports io.heddle.security;
    exports io.heddle.transaction;
    exports io.heddle.util;
}
