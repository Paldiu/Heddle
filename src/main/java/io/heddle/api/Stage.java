package io.heddle.api;

/**
 * Core processing contract for a single pipeline stage.
 *
 * <p>A {@code Stage} receives exactly one item at a time and emits zero or more output
 * items through the supplied {@link StageContext}. All stage instances run on dedicated
 * virtual threads; blocking inside {@link #process(Owned, StageContext)} is safe and
 * will not pin a carrier thread.
 *
 * <p>{@code Stage} is a functional interface. Stateless one-to-one transformations are
 * most concisely expressed as lambdas or method references via
 * {@link io.heddle.Pipeline#map(java.util.function.Function)}. Implement {@code Stage}
 * directly when you need:
 * <ul>
 *   <li>one-to-many output (fan-out), as in a flat-map</li>
 *   <li>stateful accumulation between items, such as batching or windowing</li>
 *   <li>a {@link #flush(StageContext)} callback to drain buffered state at end-of-stream</li>
 * </ul>
 *
 * <p>Implementations <em>must not</em> retain a reference to {@code item} after
 * {@link #process(Owned, StageContext)} returns. The ownership model enforced by
 * {@link Owned} will throw {@link Owned.OwnershipViolationException} if a consumed
 * item is accessed again.
 *
 * <pre>{@code
 * // Stateless stage that uppercases strings
 * Stage<String, String> upper =
 *     (item, ctx) -> ctx.emit(item.consume().toUpperCase());
 *
 * // Stateful batching stage
 * Stage<String, List<String>> batcher = new Stage<>() {
 *     private final List<String> buf = new ArrayList<>();
 *
 *     @Override
 *     public void process(Owned<String> item, StageContext<List<String>> ctx) {
 *         buf.add(item.consume());
 *         if (buf.size() >= 100) flush(ctx);
 *     }
 *
 *     @Override
 *     public void flush(StageContext<List<String>> ctx) {
 *         if (!buf.isEmpty()) { ctx.emit(new ArrayList<>(buf)); buf.clear(); }
 *     }
 * };
 * }</pre>
 *
 * @param <I> the input element type consumed by this stage
 * @param <O> the output element type emitted by this stage
 * @see io.heddle.Pipeline#stage(Stage)
 * @see StageContext
 * @see Owned
 */
@FunctionalInterface
public interface Stage<I, O> {

    /**
     * Processes one item, emitting zero or more output items through {@code ctx}.
     *
     * <p>Call {@link Owned#consume()} exactly once to retrieve the item's value.
     * After {@code consume()} is called the {@code Owned} wrapper is invalidated;
     * any subsequent access throws {@link Owned.OwnershipViolationException}. Do not
     * retain a reference to {@code item} beyond the scope of this method.
     *
     * @param item the owned input item; must be consumed exactly once via
     *             {@link Owned#consume()}
     * @param ctx  the emission context used to send output items downstream
     */
    void process(Owned<I> item, StageContext<O> ctx);

    /**
     * Called once after the upstream signals end-of-stream, before the completion
     * token propagates to the downstream stage.
     *
     * <p>Override this method to flush any state accumulated across multiple
     * {@link #process(Owned, StageContext)} calls, such as emitting a partial batch or
     * closing an aggregate. Items emitted here are delivered downstream before the
     * pipeline completes. The default implementation is a no-op and is correct for
     * stateless stages.
     *
     * @param ctx the emission context; items emitted here are delivered downstream
     *            before the pipeline reaches its terminal state
     */
    default void flush(StageContext<O> ctx) {}
}
