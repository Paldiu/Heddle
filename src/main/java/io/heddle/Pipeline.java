package io.heddle;

import io.heddle.api.Describable;
import io.heddle.api.PipelineHandle;
import io.heddle.api.Sink;
import io.heddle.api.Stage;
import io.heddle.config.PipelineOptions;
import io.heddle.error.ErrorStrategy;
import io.heddle.error.HeddleException;
import io.heddle.internal.SourceDescriptor;
import io.heddle.internal.processor.*;
import io.heddle.internal.routing.BroadcastProcessor;
import io.heddle.interop.HeddleQueue;
import io.heddle.policies.BackpressurePolicy;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Fluent pipeline builder.
 *
 * <p>Each operator appends a {@link Stage} to an internal list; no virtual threads are
 * started until a terminal operator is called. Instances of {@code Pipeline} are
 * effectively immutable: each operator returns a new {@code Pipeline} with the updated
 * stage list, leaving the original unchanged. This makes it safe to branch and reuse
 * intermediate pipeline definitions.
 *
 * <p>Operators fall into two categories:
 * <ul>
 *   <li><em>Intermediate operators</em> ({@link #map}, {@link #filter}, {@link #batch},
 *       etc.) return a new {@code Pipeline}.</li>
 *   <li><em>Terminal operators</em> ({@link #sink(Sink)}, {@link #toList},
 *       {@link #forEach}, etc.) assemble the stage graph and return either a
 *       {@link PipelineHandle} (non-blocking) or a result (blocking).</li>
 * </ul>
 *
 * <p>Non-blocking terminals return a {@link PipelineHandle}; call {@code .start()} on
 * it to begin execution. Blocking terminals start the pipeline and block until it drains.
 *
 * @param <T> the element type flowing through this point in the pipeline
 * @see Heddle
 */
@SuppressWarnings("unchecked")
public final class Pipeline<T> {

    private final SourceDescriptor<Object> source;
    private final List<Stage<?, ?>> stages;
    private final List<ErrorStrategy> strategies;
    private final PipelineOptions options;
    private final String sourceDescription;

    Pipeline(SourceDescriptor<?> source) {
        this(source, "Pipeline");
    }

    Pipeline(SourceDescriptor<?> source, String sourceDescription) {
        this.source            = (SourceDescriptor<Object>) source;
        this.sourceDescription = sourceDescription;
        this.stages            = new ArrayList<>();
        this.strategies        = new ArrayList<>();
        this.options           = PipelineOptions.defaults();
    }

    private Pipeline(SourceDescriptor<?> source, List<Stage<?, ?>> stages,
                     List<ErrorStrategy> strategies, PipelineOptions options,
                     String sourceDescription) {
        this.source            = (SourceDescriptor<Object>) source;
        this.stages            = new ArrayList<>(stages);
        this.strategies        = new ArrayList<>(strategies);
        this.options           = options;
        this.sourceDescription = sourceDescription;
    }

    /**
     * Transforms each element by applying the given function.
     *
     * @param <R> the output element type
     * @param fn  the mapping function to apply to each element; must not be {@code null}
     * @return a new {@code Pipeline} whose elements are the results of applying {@code fn}
     */
    public <R> Pipeline<R> map(Function<? super T, ? extends R> fn) {
        return appendTyped(new MapProcessor<>(fn));
    }

    /**
     * Transforms each element by applying the given function with bounded concurrency.
     *
     * <p>Up to {@code concurrency} elements are processed concurrently by the internal
     * flat-map stage; results are forwarded downstream as they complete. Useful for
     * parallelizing I/O-bound transformations without introducing a separate stage.
     *
     * @param <R>         the output element type
     * @param fn          the mapping function to apply to each element; must not be
     *                    {@code null}
     * @param concurrency the maximum number of elements processed concurrently;
     *                    must be positive
     * @return a new {@code Pipeline} whose elements are the results of applying {@code fn}
     *         with the given concurrency
     */
    public <R> Pipeline<R> mapAsync(Function<? super T, ? extends R> fn, int concurrency) {
        Function<Object, Iterable<Object>> wrapped =
                item -> Collections.singletonList(fn.apply((T) item));
        return appendTyped(new FlatMapProcessor<>(wrapped, concurrency));
    }

    /**
     * Retains only elements for which the given predicate returns {@code true}.
     *
     * @param predicate the predicate to apply to each element; must not be {@code null}
     * @return a new {@code Pipeline} consisting of elements that satisfy {@code predicate}
     */
    public Pipeline<T> filter(Predicate<? super T> predicate) {
        return append(new FilterProcessor<>(predicate));
    }

    /**
     * Retains only elements that are instances of the given type and casts them.
     *
     * <p>Elements that are not instances of {@code type} are silently discarded.
     *
     * @param <R>  the target type
     * @param type the class to filter and cast by; must not be {@code null}
     * @return a new {@code Pipeline} containing only elements that are instances of
     *         {@code type}, cast to {@code R}
     */
    public <R> Pipeline<R> filterType(Class<R> type) {
        return appendTyped(new FilterTypeProcessor<>(type));
    }

    /**
     * Transforms each element into zero or more output elements by applying the given
     * function and flattening the results into the downstream pipeline.
     *
     * <p>Elements are processed sequentially (concurrency of one). For concurrent
     * flat-mapping, use {@link #flatMap(Function, int)}.
     *
     * @param <R> the output element type
     * @param fn  a function that maps each element to an {@link Iterable} of output
     *            elements; must not be {@code null}
     * @return a new {@code Pipeline} whose elements are the concatenation of the iterables
     *         produced by {@code fn}
     */
    public <R> Pipeline<R> flatMap(Function<? super T, ? extends Iterable<? extends R>> fn) {
        return flatMap(fn, 1);
    }

    /**
     * Transforms each element into zero or more output elements with the given bounded
     * concurrency, flattening the results into the downstream pipeline.
     *
     * @param <R>         the output element type
     * @param fn          a function that maps each element to an {@link Iterable} of
     *                    output elements; must not be {@code null}
     * @param concurrency the maximum number of elements processed concurrently;
     *                    must be positive
     * @return a new {@code Pipeline} whose elements are the flattened results of
     *         applying {@code fn}
     */
    public <R> Pipeline<R> flatMap(Function<? super T, ? extends Iterable<? extends R>> fn, int concurrency) {
        return appendTyped(new FlatMapProcessor<>(
                (Function<Object, Iterable<Object>>) (Function<?, ?>) fn, concurrency));
    }

    /**
     * Groups elements into fixed-size lists.
     *
     * <p>Downstream receives {@code List<T>} instances of exactly {@code size} elements,
     * except for the final batch which may contain fewer elements if the upstream does not
     * divide evenly.
     *
     * @param size the number of elements per batch; must be positive
     * @return a new {@code Pipeline} whose elements are lists of up to {@code size} items
     */
    public Pipeline<List<T>> batch(int size) {
        return appendTyped(new BatchProcessor<>(size));
    }

    /**
     * Groups elements into time-based windows.
     *
     * <p>Downstream receives {@code List<T>} instances containing all elements that
     * arrived within each {@code duration} window. A window is emitted when the duration
     * elapses, even if it is empty.
     *
     * @param duration the length of each time window; must not be {@code null}
     * @return a new {@code Pipeline} whose elements are time-bounded lists
     */
    public Pipeline<List<T>> window(Duration duration) {
        return appendTyped(new WindowProcessor<>(duration));
    }

    /**
     * Introduces a bounded buffer between the upstream and downstream stages.
     *
     * <p>The buffer absorbs bursts from a fast producer, reducing the frequency at which
     * the producer blocks on backpressure. Increasing the buffer size trades heap memory
     * for throughput smoothness.
     *
     * @param size the capacity of the buffer in number of elements; must be positive
     * @return a new {@code Pipeline} with a buffer of the given capacity inserted
     */
    public Pipeline<T> buffer(int size) {
        return append(new BufferProcessor<>(size));
    }

    /**
     * Applies the given {@link BackpressurePolicy} when the downstream channel is full.
     *
     * <p>The default pipeline behaviour is {@link BackpressurePolicy#BLOCK}. Use this
     * operator to switch to {@link BackpressurePolicy#DROP} or
     * {@link BackpressurePolicy#FAIL_FAST} at any point in the pipeline.
     *
     * @param policy the backpressure policy to apply; must not be {@code null}
     * @return a new {@code Pipeline} with the specified backpressure policy applied
     */
    public Pipeline<T> backpressure(BackpressurePolicy policy) {
        return append(new BackpressurePolicyProcessor<>(policy));
    }

    /**
     * Limits the pipeline to the first {@code n} elements.
     *
     * <p>After {@code n} elements have been delivered downstream, the source is
     * gracefully stopped. If the upstream produces fewer than {@code n} elements, all
     * of them pass through.
     *
     * @param n the maximum number of elements to pass downstream; must be non-negative
     * @return a new {@code Pipeline} that emits at most {@code n} elements
     * @throws IllegalArgumentException if {@code n} is negative
     */
    public Pipeline<T> limit(long n) {
        if (n < 0) throw new IllegalArgumentException("n must be non-negative");
        return append(new LimitProcessor<>(n));
    }

    /**
     * Drops the first {@code n} elements and passes all subsequent elements downstream.
     *
     * <p>If the upstream produces fewer than {@code n} elements, no elements are emitted.
     *
     * @param n the number of leading elements to discard; must be non-negative
     * @return a new {@code Pipeline} that skips the first {@code n} elements
     */
    public Pipeline<T> skip(long n) {
        return append(new SkipProcessor<>(n));
    }

    /**
     * Limits throughput to at most {@code maxPerSecond} elements per second.
     *
     * <p>The stage virtual thread sleeps between emissions as needed to maintain the
     * rate limit. Sleeping releases the carrier thread, so other virtual threads are
     * not affected.
     *
     * @param maxPerSecond the maximum number of elements to emit per second;
     *                     must be positive
     * @return a new {@code Pipeline} with throughput limited to {@code maxPerSecond}
     *         elements per second
     */
    public Pipeline<T> throttle(int maxPerSecond) {
        return append(new ThrottleProcessor<>(maxPerSecond));
    }

    /**
     * Suppresses duplicate elements, using {@link Object#equals} and
     * {@link Object#hashCode} to determine equality.
     *
     * <p>The full set of seen elements is retained in memory. For pipelines with a large
     * or unbounded number of distinct elements, prefer {@link #distinct(int)} with a
     * bounded set size.
     *
     * @return a new {@code Pipeline} that emits each element at most once
     */
    public Pipeline<T> distinct() {
        return append(new DistinctProcessor<>());
    }

    /**
     * Suppresses elements that produce the same key as a previously seen element.
     *
     * <p>The key extractor is applied to each element; elements whose key has already
     * been seen are discarded. The full set of seen keys is retained in memory.
     *
     * @param <K>          the key type
     * @param keyExtractor a function that derives a deduplication key from each element;
     *                     must not be {@code null}
     * @return a new {@code Pipeline} that emits only elements with distinct keys
     */
    public <K> Pipeline<T> distinct(Function<? super T, ? extends K> keyExtractor) {
        return append(new DistinctProcessor<>(keyExtractor));
    }

    /**
     * Suppresses duplicate elements, retaining at most {@code maxKeys} distinct values
     * in memory.
     *
     * <p>When the seen-keys set reaches {@code maxKeys}, subsequent elements whose key
     * has not been seen before are passed through without deduplication (the set is full
     * and cannot track them). Use {@link #distinct(int, boolean)} to fail instead of
     * passing through.
     *
     * @param maxKeys the maximum number of distinct keys to track in memory;
     *                must be positive
     * @return a new {@code Pipeline} that suppresses duplicates up to {@code maxKeys}
     *         distinct values
     */
    public Pipeline<T> distinct(int maxKeys) {
        return append(new DistinctProcessor<>(maxKeys));
    }

    /**
     * Suppresses duplicate elements with a bounded memory budget.
     *
     * <p>When the seen-keys set reaches {@code maxKeys}, behaviour depends on
     * {@code failFast}: if {@code true}, the pipeline fails with an exception; if
     * {@code false}, new unseen elements are passed through without deduplication.
     *
     * @param maxKeys  the maximum number of distinct keys to track; must be positive
     * @param failFast if {@code true}, fail the pipeline when the key set is exhausted;
     *                 if {@code false}, pass unseen elements through without deduplication
     * @return a new {@code Pipeline} with bounded deduplication
     */
    public Pipeline<T> distinct(int maxKeys, boolean failFast) {
        return append(new DistinctProcessor<>(maxKeys, failFast));
    }

    /**
     * Performs the given action on each element as it passes through, without altering
     * the element or the pipeline's element type.
     *
     * <p>This is the primary debugging hook: use it to log, count, or inspect elements
     * mid-pipeline without affecting the downstream data.
     *
     * @param action the action to perform on each element; must not be {@code null}
     * @return a new {@code Pipeline} that passes all elements unchanged after invoking
     *         {@code action}
     */
    public Pipeline<T> peek(Consumer<? super T> action) {
        return append(new PeekProcessor<>(action));
    }

    /**
     * Logs each element to standard output using the element's {@link Object#toString()}
     * representation.
     *
     * @return a new {@code Pipeline} that prints each element and passes it downstream
     *         unchanged
     */
    public Pipeline<T> log() {
        return append(new LogProcessor<>(""));
    }

    /**
     * Logs each element to standard output, prefixing each line with {@code label}.
     *
     * @param label the label to prepend to each log line; must not be {@code null}
     * @return a new {@code Pipeline} that logs each element with the given label and
     *         passes it downstream unchanged
     */
    public Pipeline<T> log(String label) {
        return append(new LogProcessor<>(label));
    }

    /**
     * Logs each element using a custom printer.
     *
     * <p>The {@code printer} is invoked for each element; it is responsible for all
     * output formatting. Use this overload when {@link Object#toString()} is insufficient
     * or when logging to a framework other than standard output.
     *
     * @param printer a consumer that formats and outputs each element; must not be
     *                {@code null}
     * @return a new {@code Pipeline} that invokes {@code printer} for each element and
     *         passes it downstream unchanged
     */
    public Pipeline<T> log(Consumer<? super T> printer) {
        return append(new LogProcessor<>(printer));
    }

    /**
     * Configures the most-recently-added stage to retry up to {@code maxAttempts} times
     * on failure before escalating.
     *
     * <p>Equivalent to {@code .onError(new ErrorStrategy.Retry(maxAttempts))}.
     *
     * @param maxAttempts the maximum number of retry attempts; must be positive
     * @return a new {@code Pipeline} with the retry strategy applied to the last stage
     */
    public Pipeline<T> retry(int maxAttempts) {
        return onError(new ErrorStrategy.Retry(maxAttempts));
    }

    /**
     * Retry with backoff between attempts.
     *
     * @param maxAttempts the maximum number of retry attempts
     * @param backoff     the delay between consecutive retry attempts
     * @throws UnsupportedOperationException always; this overload is not yet implemented
     */
    public Pipeline<T> retry(int maxAttempts, Duration backoff) {
        throw new UnsupportedOperationException(
                "retry() with backoff requires time infrastructure; not yet implemented");
    }

    /**
     * Configures the most-recently-added stage to fail with a
     * {@link io.heddle.error.TimeoutException} if a single item takes longer than
     * {@code d} to process.
     *
     * <p>Per-item timeouts are recoverable: the exception routes through the stage's
     * {@link ErrorStrategy}. Combine with {@link #onError(ErrorStrategy)} to skip or
     * dead-letter timed-out items rather than halting the pipeline.
     *
     * @param d the maximum allowed processing time per item; must not be {@code null}
     * @return a new {@code Pipeline} with a per-item timeout applied to the last stage
     */
    public Pipeline<T> timeout(Duration d) {
        return append(new TimeoutProcessor<>(d));
    }

    /**
     * Replaces the error strategy of the most-recently-added stage.
     *
     * <p>The default strategy for every stage is {@link ErrorStrategy.Stop}. Call this
     * method immediately after the stage operator whose error handling you want to
     * customize:
     *
     * <pre>{@code
     * pipeline.map(riskyFn)
     *         .onError(new ErrorStrategy.Skip())
     *         .filter(isValid);
     * }</pre>
     *
     * @param strategy the error strategy to apply to the last stage; must not be
     *                 {@code null}
     * @return a new {@code Pipeline} with the updated error strategy
     * @throws IllegalStateException if no stage has been added yet (no preceding stage
     *                               to apply the strategy to)
     */
    public Pipeline<T> onError(ErrorStrategy strategy) {
        if (stages.isEmpty())
            throw new IllegalStateException("onError() requires a preceding stage operator");
        List<ErrorStrategy> nextStrategies = new ArrayList<>(strategies);
        nextStrategies.set(nextStrategies.size() - 1, strategy);
        return new Pipeline<>(source, stages, nextStrategies, options, sourceDescription);
    }

    /**
     * Configures a consumer-based error handler on the most-recently-added stage.
     *
     * @param handler the handler to invoke when the stage throws
     * @throws UnsupportedOperationException always; use
     *                                       {@link ErrorStrategy#deadLetterTo(io.heddle.api.DeadLetterSink)}
     *                                       instead
     */
    public Pipeline<T> onError(Consumer<Throwable> handler) {
        throw new UnsupportedOperationException(
                "onError(Consumer) not yet implemented; use onError(ErrorStrategy.deadLetterTo(...))");
    }

    /**
     * Returns a new {@code Pipeline} that uses the given {@link PipelineOptions} for
     * all subsequently assembled stages.
     *
     * @param opts the options to apply; must not be {@code null}
     * @return a new {@code Pipeline} with the given options
     */
    public Pipeline<T> withOptions(PipelineOptions opts) {
        return new Pipeline<>(source, stages, strategies, opts, sourceDescription);
    }

    /**
     * Sets the base name used when naming virtual threads for this pipeline.
     *
     * <p>Equivalent to
     * {@code withOptions(options.withThreadName(name))}.
     *
     * @param name the thread base name; must not be blank
     * @return a new {@code Pipeline} with the updated thread name
     */
    public Pipeline<T> named(String name) {
        return withOptions(options.withThreadName(name));
    }

    /**
     * Delivers each element to all given consumers before passing it downstream
     * unchanged.
     *
     * <p>Consumers are invoked synchronously on the stage virtual thread, in declaration
     * order. For asynchronous fan-out that does not block the main pipeline, use
     * {@link #tee(Consumer)}.
     *
     * @param consumers the consumers to invoke for each element; must not be {@code null}
     * @return a new {@code Pipeline} that broadcasts each element to all consumers and
     *         passes it downstream unchanged
     */
    @SafeVarargs
    public final Pipeline<T> broadcast(Consumer<? super T>... consumers) {
        Consumer<T>[] cast = (Consumer<T>[]) consumers;
        return append(new BroadcastProcessor<>(cast));
    }

    /**
     * Sends each element to {@code branch} asynchronously, then continues the main
     * pipeline unchanged.
     *
     * <p>Items are buffered through an internal {@link HeddleQueue} so the main pipeline
     * virtual thread is never blocked by the branch consumer. Errors inside the branch
     * are isolated and do not fail the main pipeline.
     *
     * @param branch the consumer to receive a copy of each element on a separate virtual
     *               thread; must not be {@code null}
     * @return a new {@code Pipeline} that asynchronously forwards each element to
     *         {@code branch} and passes it downstream unchanged
     */
    public Pipeline<T> tee(Consumer<? super T> branch) {
        HeddleQueue<T> bridge = new HeddleQueue<>();
        PipelineHandle branchHandle = bridge.asSource().sink(branch);
        return append(new AsyncTeeProcessor<>(bridge.asSink(), branchHandle));
    }

    /**
     * Forks a full branch pipeline from this point in the main pipeline.
     *
     * <p>The {@code branchSetup} function receives a {@link Pipeline}{@code <T>} sourced
     * from an internal {@link HeddleQueue} bridge and must return an assembled (but not
     * yet started) {@link PipelineHandle}, typically by calling {@code .sink(...)} or a
     * blocking terminal on the branch pipeline.
     *
     * <p>The branch starts when the main pipeline starts, runs concurrently on its own
     * virtual threads, and is cancelled automatically if the main pipeline is cancelled.
     * When the main pipeline completes, end-of-stream is signalled to the branch so it
     * drains cleanly.
     *
     * <pre>{@code
     * Heddle.fromLines(path)
     *       .map(parse)
     *       .tee(branch -> branch.filter(Event::isError).sink(errorLog))
     *       .filter(Event::isValid)
     *       .forEach(db::insert);
     * }</pre>
     *
     * @param branchSetup a function that receives the branch source pipeline and returns
     *                    an assembled {@code PipelineHandle} for the branch; must not be
     *                    {@code null}
     * @return a new {@code Pipeline} that asynchronously forks each element to the branch
     *         and passes it downstream unchanged
     */
    public Pipeline<T> tee(java.util.function.Function<Pipeline<T>, PipelineHandle> branchSetup) {
        HeddleQueue<T> bridge = new HeddleQueue<>();
        PipelineHandle branchHandle = branchSetup.apply(bridge.asSource());
        return append(new AsyncTeeProcessor<>(bridge.asSink(), branchHandle));
    }

    /**
     * Appends a user-supplied {@link Stage} to the pipeline.
     *
     * <p>Use this method to integrate custom processing logic that does not fit any of
     * the built-in operators. The stage may be stateful and may emit zero or more items
     * per input element.
     *
     * @param <R>   the output element type produced by the stage
     * @param stage the custom stage to add; must not be {@code null}
     * @return a new {@code Pipeline} with the given stage appended
     */
    public <R> Pipeline<R> stage(Stage<? super T, ? extends R> stage) {
        return appendTyped(stage);
    }

    /**
     * Assembles the pipeline with a simple consumer as the terminal sink.
     *
     * <p>This is a non-blocking terminal operator. The returned {@link PipelineHandle}
     * must be started explicitly by calling {@link PipelineHandle#start()}.
     *
     * @param consumer the terminal consumer to receive each element;
     *                 must not be {@code null}
     * @return a {@code PipelineHandle} for the assembled pipeline; not yet started
     */
    public PipelineHandle sink(Consumer<? super T> consumer) {
        return sink(new Sink<T>() {
            @Override public void accept(T item) { consumer.accept(item); }
        });
    }

    /**
     * Assembles the pipeline with the given {@link Sink} as the terminal consumer.
     *
     * <p>This is a non-blocking terminal operator. The returned {@link PipelineHandle}
     * must be started explicitly by calling {@link PipelineHandle#start()}.
     *
     * @param userSink the terminal sink; must not be {@code null}
     * @return a {@code PipelineHandle} for the assembled pipeline; not yet started
     */
    public PipelineHandle sink(Sink<T> userSink) {
        return HeddleCore.assembleFull(source, stages, strategies, userSink, options);
    }

    /**
     * Collects all elements into a {@link List} and returns it.
     *
     * <p>This is a blocking terminal operator. It starts the pipeline and blocks the
     * calling thread until the pipeline completes.
     *
     * @return a {@code List} containing all elements emitted by the pipeline, in
     *         encounter order
     */
    public List<T> toList() {
        return toCollection(ArrayList::new);
    }

    /**
     * Collects all elements into a collection produced by the given factory.
     *
     * <p>This is a blocking terminal operator. It starts the pipeline and blocks the
     * calling thread until the pipeline completes.
     *
     * @param <C>     the collection type
     * @param factory a supplier that creates the target collection; must not be
     *                {@code null}
     * @return the collection containing all elements, as produced by {@code factory}
     */
    public <C extends Collection<T>> C toCollection(Supplier<C> factory) {
        C result = factory.get();
        Sink<T> collectSink = new Sink<T>() {
            @Override public void accept(T item) { result.add(item); }
        };
        PipelineHandle handle = sink(collectSink);
        handle.start();
        handle.awaitCompletion();
        throwIfFailed(handle);
        return result;
    }

    /**
     * Returns the first element emitted by the pipeline, if any.
     *
     * <p>This is a blocking terminal operator. The pipeline is started and blocks the
     * calling thread. Once the first element is captured, the source is stopped
     * gracefully; remaining elements in transit may or may not be delivered to the sink.
     *
     * @return an {@code Optional} containing the first element, or an empty
     *         {@code Optional} if the pipeline emits no elements
     */
    public Optional<T> first() {
        AtomicReference<T> holder = new AtomicReference<>();
        Sink<T> firstSink = new Sink<T>() {
            private boolean done = false;
            @Override public void accept(T item) {
                if (!done) { holder.compareAndSet(null, item); done = true; }
            }
        };
        PipelineHandle handle = sink(firstSink);
        handle.start();
        handle.awaitCompletion();
        throwIfFailed(handle);
        return Optional.ofNullable(holder.get());
    }

    /**
     * Counts the total number of elements emitted by the pipeline.
     *
     * <p>This is a blocking terminal operator. It starts the pipeline and blocks the
     * calling thread until the pipeline completes.
     *
     * @return the total number of elements emitted
     */
    public long count() {
        AtomicLong counter = new AtomicLong();
        Sink<T> countSink = new Sink<T>() {
            @Override public void accept(T item) { counter.incrementAndGet(); }
        };
        PipelineHandle handle = sink(countSink);
        handle.start();
        handle.awaitCompletion();
        throwIfFailed(handle);
        return counter.get();
    }

    /**
     * Collects all elements using the given {@link Collector}.
     *
     * <p>This is a blocking terminal operator. It starts the pipeline and blocks the
     * calling thread until the pipeline completes.
     *
     * @param <A>       the intermediate accumulation type of the collector
     * @param <R>       the result type of the collector
     * @param collector the collector to use; must not be {@code null}
     * @return the result produced by the collector's finisher
     */
    public <A, R> R collect(Collector<? super T, A, R> collector) {
        A container = collector.supplier().get();
        BiConsumer<A, ? super T> accumulator = collector.accumulator();
        Sink<T> collectSink = new Sink<T>() {
            @Override public void accept(T item) { accumulator.accept(container, item); }
        };
        PipelineHandle handle = sink(collectSink);
        handle.start();
        handle.awaitCompletion();
        throwIfFailed(handle);
        return collector.finisher().apply(container);
    }

    /**
     * Collects all elements into a {@link Map} using the given key and value mapping
     * functions.
     *
     * <p>This is a blocking terminal operator. Throws {@link IllegalStateException} if
     * two elements produce the same key.
     *
     * @param <K>         the key type
     * @param <V>         the value type
     * @param keyMapper   a function to derive the map key from each element;
     *                    must not be {@code null}
     * @param valueMapper a function to derive the map value from each element;
     *                    must not be {@code null}
     * @return a {@code Map} whose entries are the result of applying the key and value
     *         mappers to each element
     * @throws IllegalStateException if duplicate keys are produced
     */
    public <K, V> Map<K, V> toMap(Function<? super T, ? extends K> keyMapper,
                                   Function<? super T, ? extends V> valueMapper) {
        return collect(Collectors.toMap(keyMapper, valueMapper));
    }

    /**
     * Groups all elements by a classifier function.
     *
     * <p>This is a blocking terminal operator.
     *
     * @param <K>        the group key type
     * @param classifier a function that derives the group key from each element;
     *                   must not be {@code null}
     * @return a {@code Map} from each distinct key to the list of elements that produced
     *         that key
     */
    public <K> Map<K, List<T>> groupBy(Function<? super T, ? extends K> classifier) {
        return collect(Collectors.groupingBy(classifier));
    }

    /**
     * Partitions all elements into two groups based on the given predicate.
     *
     * <p>This is a blocking terminal operator.
     *
     * @param predicate the partitioning predicate; must not be {@code null}
     * @return a {@code Map} with exactly two entries keyed by {@code true} (elements
     *         satisfying the predicate) and {@code false} (elements that do not)
     */
    public Map<Boolean, List<T>> partition(Predicate<? super T> predicate) {
        return collect(Collectors.partitioningBy(predicate));
    }

    /**
     * Reduces all elements to a single value using the given associative binary operator.
     *
     * <p>This is a blocking terminal operator. Elements are reduced in encounter order.
     * If no elements are emitted, the result is an empty {@code Optional}.
     *
     * @param reducer an associative, non-interfering, stateless function for combining
     *                two elements; must not be {@code null}
     * @return an {@code Optional} containing the reduced value, or an empty
     *         {@code Optional} if no elements were emitted
     */
    public Optional<T> reduce(BinaryOperator<T> reducer) {
        AtomicReference<T> acc = new AtomicReference<>();
        Sink<T> reduceSink = new Sink<T>() {
            @Override public void accept(T item) {
                acc.accumulateAndGet(item, (a, b) -> a == null ? b : reducer.apply(a, b));
            }
        };
        PipelineHandle handle = sink(reduceSink);
        handle.start();
        handle.awaitCompletion();
        throwIfFailed(handle);
        return Optional.ofNullable(acc.get());
    }

    /**
     * Performs the given action on each element in encounter order.
     *
     * <p>This is a blocking terminal operator. It starts the pipeline and blocks the
     * calling thread until the pipeline completes.
     *
     * @param action the action to perform on each element; must not be {@code null}
     */
    public void forEach(Consumer<? super T> action) {
        Sink<T> forEachSink = new Sink<T>() {
            @Override public void accept(T item) { action.accept(item); }
        };
        PipelineHandle handle = sink(forEachSink);
        handle.start();
        handle.awaitCompletion();
        throwIfFailed(handle);
    }

    /**
     * Runs the pipeline to completion, discarding all elements.
     *
     * <p>This is a blocking terminal operator. Use it when only side-effects in
     * intermediate stages matter and no elements need to be collected.
     */
    public void drain() {
        Sink<T> drainSink = new Sink<T>() {
            @Override public void accept(T item) {}
        };
        PipelineHandle handle = sink(drainSink);
        handle.start();
        handle.awaitCompletion();
        throwIfFailed(handle);
    }

    private static void throwIfFailed(PipelineHandle handle) {
        if (handle.isFailed()) {
            Throwable cause = handle.failureCause().orElse(new HeddleException("pipeline failed"));
            if (cause instanceof RuntimeException re) throw re;
            throw new HeddleException("pipeline failed", cause);
        }
    }

    /**
     * Unwraps a pipeline of {@link CompletableFuture}{@code <R>} items into their
     * resolved values.
     *
     * <p>Each future is awaited by parking the stage virtual thread on
     * {@link CompletableFuture#get()}: no callbacks and no continuations are used. The
     * carrier thread is released while the virtual thread waits, so hundreds of in-flight
     * futures cost no platform threads. This is the canonical Project Loom idiom: treat
     * async values as data and block on them directly.
     *
     * @param <R> the resolved value type of the futures in the upstream pipeline
     * @return a new {@code Pipeline} whose elements are the resolved values of the
     *         upstream futures, in the order they were emitted
     */
    public <R> Pipeline<R> awaitFutures() {
        return appendTyped(new FutureUnwrapProcessor<>());
    }

    /**
     * Starts the pipeline and returns a {@link CompletableFuture} that completes when
     * all elements have been collected.
     *
     * <p>This is a non-blocking terminal operator. The pipeline is started immediately
     * on virtual threads; the returned future completes normally with the collected list
     * of elements, or exceptionally if the pipeline fails. Use this to integrate Heddle
     * into existing {@link CompletableFuture}-based orchestration without blocking the
     * caller.
     *
     * @return a {@code CompletableFuture} that completes with a list of all collected
     *         elements, or completes exceptionally on pipeline failure
     */
    public CompletableFuture<List<T>> toFuture() {
        CompletableFuture<List<T>> future = new CompletableFuture<>();
        List<T> results = Collections.synchronizedList(new ArrayList<>());

        Sink<T> collectSink = new Sink<T>() {
            @Override public void accept(T item) { results.add(item); }
            @Override public void onComplete() { future.complete(results); }
            @Override public void onError(Throwable cause) { future.completeExceptionally(cause); }
        };

        PipelineHandle handle = sink(collectSink);
        handle.start();
        return future;
    }

    /**
     * Returns a multi-line, human-readable description of this pipeline's source and
     * stages.
     *
     * <p>Stages that implement {@link Describable} contribute their own description
     * string; others fall back to their simple class name. Example output:
     *
     * <pre>
     * Source: File[data.csv]
     * Stage 0: MapProcessor
     * Stage 1: FilterProcessor
     * Stage 2: BatchProcessor(100)
     * </pre>
     *
     * @return a non-{@code null} multi-line string describing the pipeline's structure
     */
    public String describe() {
        StringBuilder sb = new StringBuilder();
        sb.append("Source: ").append(sourceDescription);
        for (int i = 0; i < stages.size(); i++) {
            Stage<?, ?> stage = stages.get(i);
            String stageName = stage instanceof Describable d
                    ? d.describe()
                    : stage.getClass().getSimpleName();
            sb.append('\n').append("Stage ").append(i).append(": ").append(stageName);
        }
        return sb.toString();
    }

    /**
     * Returns a new {@code Pipeline} identical to this one but with the source
     * description label replaced by the given string.
     *
     * <p>The label appears in the output of {@link #describe()} and in thread names.
     * Used by {@link Heddle} factory methods that delegate internally to avoid the
     * generic label appearing in diagnostics.
     *
     * @param description the new source description label; must not be {@code null}
     * @return a new {@code Pipeline} with the updated source label
     */
    public Pipeline<T> describedAs(String description) {
        return new Pipeline<>(source, stages, strategies, options, description);
    }

    private Pipeline<T> append(Stage<?, ?> stage) {
        List<Stage<?, ?>> next = new ArrayList<>(stages);
        next.add(stage);
        List<ErrorStrategy> nextStrategies = new ArrayList<>(strategies);
        nextStrategies.add(new ErrorStrategy.Stop());
        return new Pipeline<>(source, next, nextStrategies, options, sourceDescription);
    }

    private <R> Pipeline<R> appendTyped(Stage<?, ?> stage) {
        List<Stage<?, ?>> next = new ArrayList<>(stages);
        next.add(stage);
        List<ErrorStrategy> nextStrategies = new ArrayList<>(strategies);
        nextStrategies.add(new ErrorStrategy.Stop());
        return new Pipeline<>(source, next, nextStrategies, options, sourceDescription);
    }
}
