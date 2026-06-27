package io.heddle.error;

/**
 * Enum representation of {@link ErrorStrategy} variants, suitable for use in Java
 * annotations.
 *
 * <p>Java annotations cannot reference sealed-interface record types directly.
 * {@code ErrorStrategyType} provides a parallel enum that may be used in annotation
 * attributes wherever an {@link ErrorStrategy} is conceptually required. At pipeline
 * assembly time, each constant is mapped to the corresponding {@link ErrorStrategy}
 * record implementation.
 *
 * @see ErrorStrategy
 */
public enum ErrorStrategyType {

    /** Corresponds to {@link ErrorStrategy.Stop}: halt the pipeline on failure. */
    STOP,

    /** Corresponds to {@link ErrorStrategy.Skip}: silently drop the failed item. */
    SKIP,

    /** Corresponds to {@link ErrorStrategy.Retry}: re-attempt the failed item. */
    RETRY,

    /** Corresponds to {@link ErrorStrategy.DeadLetter}: route the failed item to a side channel. */
    DEAD_LETTER
}
