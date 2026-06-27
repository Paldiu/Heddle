package io.heddle.api;

/**
 * Implemented by pipeline stages and processors that can produce a concise,
 * human-readable description of their configuration.
 *
 * <p>{@link io.heddle.Pipeline#describe()} uses this interface to build a diagnostic
 * representation of the pipeline without resorting to reflection. Stages that do not
 * implement {@code Describable} fall back to their simple class name in the output.
 *
 * <p>Descriptions should be single-line strings that include key configuration
 * parameters, for example:
 * <ul>
 *   <li>{@code "BatchProcessor(100)"}</li>
 *   <li>{@code "DistinctProcessor(maxKeys=50000)"}</li>
 *   <li>{@code "FilterTypeProcessor(java.lang.String)"}</li>
 * </ul>
 *
 * @see io.heddle.Pipeline#describe()
 */
public interface Describable {

    /**
     * Returns a concise, human-readable description of this stage's configuration.
     *
     * @return a non-{@code null} single-line description string
     */
    String describe();
}
