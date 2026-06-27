package io.heddle.api;

/**
 * Implemented by classes that encapsulate a complete pipeline using the fluent
 * builder API.
 *
 * <p>{@code PipelineDefinition} promotes the class-based style of pipeline authoring,
 * where each transformation step is a method on the enclosing class. Use method
 * references ({@code this::methodName}) to bind the class's own methods as stages,
 * keeping logic encapsulated without annotation scanning or reflection.
 *
 * <p>Once implemented, a definition can be assembled and started via
 * {@link io.heddle.Heddle#wire(PipelineDefinition)} (assemble without starting) or
 * {@link io.heddle.Heddle#run(PipelineDefinition)} (assemble and start immediately):
 *
 * <pre>{@code
 * public class IngestPipeline implements PipelineDefinition {
 *
 *     @Override
 *     public PipelineHandle assemble() {
 *         return Heddle.from(this::readSource)
 *                 .map(this::parse)
 *                 .filter(this::validate)
 *                 .batch(100)
 *                 .sink(this::persist);
 *     }
 *
 *     private void readSource(Emitter<String> emit) {
 *         Files.lines(Path.of("input.txt")).forEach(emit::emit);
 *     }
 *
 *     private Record parse(String line)        { return CsvParser.parse(line); }
 *     private boolean validate(Record r)       { return r.isValid(); }
 *     private void persist(List<Record> batch) { db.insertBatch(batch); }
 * }
 *
 * // Wire without starting (useful for testing or deferred start):
 * PipelineHandle handle = Heddle.wire(new IngestPipeline());
 * handle.start();
 *
 * // Wire and start immediately:
 * PipelineHandle running = Heddle.run(new IngestPipeline());
 * running.awaitCompletion();
 * }</pre>
 *
 * @see io.heddle.Heddle#wire(PipelineDefinition)
 * @see io.heddle.Heddle#run(PipelineDefinition)
 * @see PipelineHandle
 */
@FunctionalInterface
public interface PipelineDefinition {

    /**
     * Assembles the pipeline and returns a {@link PipelineHandle} that can be used
     * to start, stop, or monitor it.
     *
     * <p>This method constructs the stage graph but does not start any virtual threads.
     * Call {@link PipelineHandle#start()} on the returned handle to begin execution.
     *
     * @return a handle representing the assembled, not-yet-started pipeline
     */
    PipelineHandle assemble();
}
