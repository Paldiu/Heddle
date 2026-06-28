package io.heddle;

import io.heddle.api.Emitter;
import io.heddle.api.PathValidator;
import io.heddle.api.PipelineHandle;
import io.heddle.api.PipelineDefinition;
import io.heddle.api.Sink;
import io.heddle.config.MergeStrategy;
import io.heddle.internal.SourceStage;
import io.heddle.internal.file.FileSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Primary entry point for the Heddle pipeline library.
 *
 * <p>{@code Heddle} exposes factory methods that create a {@link Pipeline} from a
 * variety of source types: collections, iterables, files, byte streams, suppliers,
 * push-style emitters, and futures. All pipelines are lazy: no virtual threads are
 * started until a terminal operator ({@link Pipeline#sink(Sink)}, {@link Pipeline#forEach},
 * {@link Pipeline#toList()}, etc.) is called and the returned {@link PipelineHandle} is
 * started.
 *
 * <h2>Fluent (method-chain) usage</h2>
 * <pre>{@code
 * Heddle.fromLines(Path.of("data.csv"))
 *       .map(CsvParser::parse)
 *       .filter(Record::isValid)
 *       .batch(100)
 *       .sink(db::insertBatch)
 *       .start();
 * }</pre>
 *
 * <h2>Class-based usage</h2>
 * <pre>{@code
 * PipelineHandle handle = Heddle.run(new IngestPipeline());
 * handle.awaitCompletion();
 * }</pre>
 *
 * <p>All methods in this class are static; the class cannot be instantiated.
 */
@SuppressWarnings("unchecked")
public final class Heddle {

    private Heddle() {}

    /**
     * Creates a pipeline sourced from the elements of the given {@link Iterable}.
     *
     * <p>The iterable is traversed by the source stage's virtual thread in encounter
     * order. Blocking iterables are safe to use: the virtual thread parks without
     * pinning a carrier thread.
     *
     * @param <T>    the element type
     * @param source the iterable to traverse; must not be {@code null}
     * @return a new {@code Pipeline} backed by the given iterable
     * @throws NullPointerException if {@code source} is {@code null}
     */
    public static <T> Pipeline<T> from(Iterable<? extends T> source) {
        if (source == null) throw new NullPointerException("source must not be null");
        return new Pipeline<>((stageId, downstream, context) ->
                SourceStage.fromIterable(stageId, source, downstream, context),
                "Iterable");
    }

    /**
     * Creates a pipeline sourced from a {@link Supplier} that is polled repeatedly
     * until it returns {@code null}.
     *
     * <p>Returning {@code null} from the supplier signals end-of-stream. This pattern
     * is suitable for wrapping blocking data sources such as JDBC cursors: the source
     * virtual thread parks on each call without pinning a carrier thread.
     *
     * @param <T>    the element type
     * @param source the supplier to poll; must not be {@code null}; return {@code null}
     *               to terminate the source
     * @return a new {@code Pipeline} driven by the given supplier
     * @throws NullPointerException if {@code source} is {@code null}
     */
    public static <T> Pipeline<T> from(Supplier<? extends T> source) {
        if (source == null) throw new NullPointerException("source must not be null");
        return new Pipeline<>((stageId, downstream, context) ->
                SourceStage.fromSupplier(stageId, source, downstream, context),
                "Supplier");
    }

    /**
     * Creates a pipeline from a fixed set of items provided as a varargs array.
     *
     * @param <T>   the element type
     * @param items the items to emit; must not be {@code null}
     * @return a new {@code Pipeline} that emits each item in declaration order
     */
    @SafeVarargs
    public static <T> Pipeline<T> of(T... items) {
        return from(List.of(items)).describedAs("Of");
    }

    /**
     * Creates a pipeline that completes immediately without emitting any items.
     *
     * @param <T> the element type
     * @return a new empty {@code Pipeline}
     */
    public static <T> Pipeline<T> empty() {
        return from(List.<T>of()).describedAs("Empty");
    }

    /**
     * Creates a single-item pipeline backed by a {@link CompletableFuture}.
     *
     * <p>The source virtual thread parks on {@link CompletableFuture#get()} without
     * pinning a carrier thread. If the future completes exceptionally, the cause is
     * propagated as a pipeline failure. Combine with {@link Pipeline#awaitFutures()}
     * to handle a collection of futures as a streaming source.
     *
     * @param <T>    the element type
     * @param future the future whose result becomes the sole pipeline item;
     *               must not be {@code null}
     * @return a new {@code Pipeline} that emits the future's result once it is available
     * @throws NullPointerException if {@code future} is {@code null}
     */
    public static <T> Pipeline<T> from(CompletableFuture<T> future) {
        if (future == null) throw new NullPointerException("future must not be null");
        return from((Emitter<T> emitter) -> {
            try {
                emitter.emit(future.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new java.util.concurrent.CancellationException("future interrupted");
            } catch (java.util.concurrent.ExecutionException e) {
                Throwable cause = e.getCause() != null ? e.getCause() : e;
                if (cause instanceof RuntimeException re) throw re;
                throw new RuntimeException("future completed exceptionally", cause);
            }
        }).describedAs("Future");
    }

    /**
     * Creates a pipeline driven by a push-style producer that emits items through an
     * {@link Emitter}.
     *
     * <p>This is the preferred pattern for infinite or event-driven sources. The
     * producer lambda runs on the source stage's virtual thread and simply returns when
     * it is done emitting. The producer's return is the implicit end-of-stream signal.
     *
     * <pre>{@code
     * Heddle.from((Emitter<Event> emit) -> {
     *     eventBus.subscribe(emit::emit);
     * });
     * }</pre>
     *
     * @param <T>      the element type
     * @param producer a callback that receives an {@link Emitter} and pushes items
     *                 through it; must not be {@code null}
     * @return a new {@code Pipeline} driven by the given producer
     * @throws NullPointerException if {@code producer} is {@code null}
     */
    public static <T> Pipeline<T> from(Consumer<? super Emitter<T>> producer) {
        if (producer == null) throw new NullPointerException("producer must not be null");
        return new Pipeline<>((stageId, downstream, context) ->
                SourceStage.fromEmitter(stageId, producer, downstream, context),
                "Emitter");
    }

    /**
     * Creates a pipeline from a {@link Stream}.
     *
     * <p>The stream is consumed exactly once by the source stage virtual thread; callers
     * must not reuse it after passing it to this method. The stream is <em>not</em>
     * closed by this method: if the stream owns resources (for example, a
     * {@link Files#lines(Path)} stream), wrap the pipeline in a try-with-resources
     * block or register an {@code onComplete} callback to close it.
     *
     * @param <T>    the element type
     * @param stream the stream to consume; must not be {@code null}; must not be reused
     *               after this call
     * @return a new {@code Pipeline} backed by the given stream
     * @throws NullPointerException if {@code stream} is {@code null}
     */
    public static <T> Pipeline<T> fromStream(Stream<T> stream) {
        if (stream == null) throw new NullPointerException("stream must not be null");
        return from((Iterable<T>) stream::iterator).describedAs("Stream");
    }

    /**
     * Creates a pipeline from an {@link Iterator}.
     *
     * <p>{@link Iterator#hasNext()} and {@link Iterator#next()} are called on the
     * source stage virtual thread. Blocking iterators (for example, those wrapping a
     * network cursor) are safe here: the virtual thread parks without pinning a carrier
     * thread.
     *
     * @param <T>      the element type
     * @param iterator the iterator to drain; must not be {@code null}
     * @return a new {@code Pipeline} backed by the given iterator
     * @throws NullPointerException if {@code iterator} is {@code null}
     */
    public static <T> Pipeline<T> fromIterator(Iterator<T> iterator) {
        if (iterator == null) throw new NullPointerException("iterator must not be null");
        return from((Iterable<T>) () -> iterator).describedAs("Iterator");
    }

    /**
     * Creates an infinite pipeline that emits a monotonically increasing {@code Long}
     * counter after each {@code interval}.
     *
     * <p>The first tick fires after one {@code interval}, not immediately. The source
     * virtual thread parks on {@link Thread#sleep} between ticks, releasing its carrier
     * thread. Cancel the pipeline via {@link io.heddle.api.PipelineHandle#cancel()} to
     * stop the source.
     *
     * @param interval the delay between consecutive ticks; must be a positive,
     *                 non-zero {@link Duration}
     * @return a new infinite {@code Pipeline} that emits {@code 1, 2, 3, ...} at the
     *         specified interval
     * @throws IllegalArgumentException if {@code interval} is {@code null}, zero, or
     *                                  negative
     */
    public static Pipeline<Long> tick(Duration interval) {
        if (interval == null || interval.isNegative() || interval.isZero())
            throw new IllegalArgumentException("interval must be a positive Duration");
        AtomicLong counter = new AtomicLong();
        return from((Supplier<Long>) () -> {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
            return counter.incrementAndGet();
        }).describedAs("Tick");
    }

    /**
     * Creates a pipeline that emits all integers in the half-open range {@code [from, to)},
     * analogous to {@link java.util.stream.IntStream#range}.
     *
     * @param from the inclusive start of the range
     * @param to   the exclusive end of the range
     * @return a new {@code Pipeline} that emits each integer from {@code from} up to,
     *         but not including, {@code to}; emits nothing if {@code from >= to}
     */
    public static Pipeline<Integer> range(int from, int to) {
        Iterable<Integer> iterable = () -> new java.util.Iterator<Integer>() {
            int current = from;
            @Override public boolean hasNext() { return current < to; }
            @Override public Integer next() {
                if (!hasNext()) throw new java.util.NoSuchElementException();
                return current++;
            }
        };
        return new Pipeline<>((stageId, downstream, context) ->
                SourceStage.fromIterable(stageId, iterable, downstream, context),
                "Range(" + from + ".." + to + ")");
    }

    /**
     * Creates an infinite pipeline that generates items by invoking the given supplier
     * indefinitely on the source stage's virtual thread.
     *
     * <p>To stop the pipeline, cancel it via
     * {@link io.heddle.api.PipelineHandle#cancel()}, or return {@code null} from the
     * supplier to signal end-of-stream.
     *
     * @param <T>      the element type
     * @param supplier the item generator; must not be {@code null}
     * @return a new infinite {@code Pipeline} backed by the given supplier
     * @throws NullPointerException if {@code supplier} is {@code null}
     */
    public static <T> Pipeline<T> generate(Supplier<T> supplier) {
        if (supplier == null) throw new NullPointerException("supplier must not be null");
        return from(supplier).describedAs("Generate");
    }

    /**
     * Creates a pipeline that streams the lines of a UTF-8 encoded text file as
     * {@code String} items, one line per item.
     *
     * <p><b>Trust boundary:</b> validate {@code path} with a {@link PathValidator}
     * before calling this method; no path canonicalization is performed here.
     *
     * @param path the path to the text file to read; must not be {@code null}
     * @return a new {@code Pipeline} that emits each line of the file, without line
     *         terminators, in file order
     * @throws NullPointerException if {@code path} is {@code null}
     * @throws UncheckedIOException if the file cannot be opened or read
     */
    public static Pipeline<String> fromLines(Path path) {
        return fromLines(path, StandardCharsets.UTF_8);
    }

    /**
     * Creates a pipeline that streams the lines of a UTF-8 encoded text file, after
     * first validating the path with the given {@link PathValidator}.
     *
     * @param path      the path to the text file to read; must not be {@code null}
     * @param validator the path validator to apply before opening the file;
     *                  if {@code null}, no validation is performed
     * @return a new {@code Pipeline} that emits each line of the file, without line
     *         terminators, in file order
     * @throws SecurityException    if {@code validator} rejects the path
     * @throws NullPointerException if {@code path} is {@code null}
     * @throws UncheckedIOException if the file cannot be opened or read
     */
    public static Pipeline<String> fromLines(Path path, PathValidator validator) {
        if (validator != null) validator.validate(path);
        return fromLines(path);
    }

    /**
     * Creates a pipeline that streams the lines of a text file with the given
     * {@link Charset}, after first validating the path with the given
     * {@link PathValidator}.
     *
     * @param path      the path to the text file to read; must not be {@code null}
     * @param charset   the character set used to decode the file; must not be {@code null}
     * @param validator the path validator to apply before opening the file;
     *                  if {@code null}, no validation is performed
     * @return a new {@code Pipeline} that emits each line of the file, without line
     *         terminators, in file order
     * @throws SecurityException    if {@code validator} rejects the path
     * @throws NullPointerException if {@code path} or {@code charset} is {@code null}
     * @throws UncheckedIOException if the file cannot be opened or read
     */
    public static Pipeline<String> fromLines(Path path, Charset charset, PathValidator validator) {
        if (validator != null) validator.validate(path);
        return fromLines(path, charset);
    }

    /**
     * Creates a pipeline that streams the lines of a text file with the given
     * {@link Charset}.
     *
     * <p><b>Trust boundary:</b> validate {@code path} with a {@link PathValidator}
     * before calling this method; no path canonicalization is performed here.
     *
     * @param path    the path to the text file to read; must not be {@code null}
     * @param charset the character set used to decode the file; must not be {@code null}
     * @return a new {@code Pipeline} that emits each line of the file, without line
     *         terminators, in file order
     * @throws NullPointerException if {@code path} or {@code charset} is {@code null}
     * @throws UncheckedIOException if the file cannot be opened or read
     */
    public static Pipeline<String> fromLines(Path path, Charset charset) {
        if (path == null)    throw new NullPointerException("path must not be null");
        if (charset == null) throw new NullPointerException("charset must not be null");
        FileSource source = new FileSource(path, charset);
        return new Pipeline<>((stageId, downstream, context) ->
                SourceStage.fromEmitter(stageId, source, downstream, context),
                "File[" + path.getFileName() + "]");
    }

    /**
     * Creates a pipeline that reads an {@link InputStream} and emits fixed-size
     * {@code byte[]} chunks.
     *
     * <p>The final chunk may be smaller than {@code chunkSize} if the total stream
     * length is not a multiple of {@code chunkSize}. The stream is not closed by this
     * method; callers are responsible for closing it after the pipeline completes.
     *
     * @param stream    the input stream to read; must not be {@code null}
     * @param chunkSize the number of bytes per chunk; must be positive
     * @return a new {@code Pipeline} that emits each chunk of bytes read from the stream
     * @throws NullPointerException     if {@code stream} is {@code null}
     * @throws IllegalArgumentException if {@code chunkSize} is not positive
     * @throws UncheckedIOException     if the stream cannot be read
     */
    public static Pipeline<byte[]> fromBytes(InputStream stream, int chunkSize) {
        if (stream == null) throw new NullPointerException("stream must not be null");
        if (chunkSize <= 0) throw new IllegalArgumentException("chunkSize must be positive");
        return new Pipeline<>((stageId, downstream, context) ->
                SourceStage.fromEmitter(stageId, emitter -> {
                    byte[] buf = new byte[chunkSize];
                    int read;
                    try {
                        while ((read = stream.read(buf)) != -1) {
                            if (Thread.currentThread().isInterrupted()) break;
                            byte[] chunk = read == chunkSize ? buf.clone()
                                    : Arrays.copyOf(buf, read);
                            emitter.emit(chunk);
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException("failed reading stream", e);
                    }
                }, downstream, context),
                "InputStream");
    }

    /**
     * Creates a pipeline by merging multiple iterables into a single sequential source.
     *
     * <p>Items from each iterable are emitted in encounter order. Iterables are drained
     * one after another on a single source virtual thread; there is no concurrent fan-in.
     * For concurrent merging of independent {@link Pipeline} instances, use
     * {@link #merge(Pipeline[])}.
     *
     * @param <T>     the element type
     * @param sources the iterables to merge; must not be {@code null}
     * @return a new {@code Pipeline} that emits all items from all sources in sequence
     * @throws NullPointerException if {@code sources} is {@code null}
     */
    @SafeVarargs
    public static <T> Pipeline<T> merge(Iterable<? extends T>... sources) {
        if (sources == null) throw new NullPointerException("sources must not be null");
        return new Pipeline<>((stageId, downstream, context) ->
                SourceStage.fromEmitter(stageId, emitter -> {
                    for (Iterable<? extends T> src : sources) {
                        for (T item : src) emitter.emit(item);
                    }
                }, downstream, context),
                "Merge");
    }

    /**
     * Creates a pipeline by merging already-assembled pipelines into a single concurrent
     * fan-in source.
     *
     * <p>All input pipelines run concurrently on their own virtual threads; items are
     * interleaved in arrival order with no ordering guarantee across sources. The merged
     * pipeline completes once every upstream has finished.
     *
     * <p>Error behaviour is controlled by
     * {@link io.heddle.config.PipelineOptions#withMergeStrategy(io.heddle.config.MergeStrategy)}:
     * <ul>
     *   <li>{@link MergeStrategy#FAIL_FAST} (default): the merged output fails as soon as
     *       any upstream fails; remaining upstreams continue to natural completion but
     *       their output is discarded.</li>
     *   <li>{@link MergeStrategy#BEST_EFFORT}: all upstreams run to completion; every error
     *       is collected and surfaced together as a single {@link RuntimeException} with
     *       suppressed causes.</li>
     * </ul>
     *
     * @param <T>       the element type
     * @param pipelines the pipelines to merge concurrently; must not be {@code null};
     *                  if empty, an empty pipeline is returned; if exactly one is given,
     *                  that pipeline is returned unchanged
     * @return a new {@code Pipeline} that emits items from all input pipelines as they
     *         arrive
     * @throws NullPointerException if {@code pipelines} is {@code null}
     */
    @SafeVarargs
    public static <T> Pipeline<T> merge(Pipeline<? extends T>... pipelines) {
        if (pipelines == null) throw new NullPointerException("pipelines must not be null");
        if (pipelines.length == 0) return empty();
        if (pipelines.length == 1) return (Pipeline<T>) pipelines[0];

        Pipeline<? extends T>[] copy = Arrays.copyOf(pipelines, pipelines.length);
        int count = copy.length;

        return new Pipeline<>((stageId, downstream, context) ->
                SourceStage.fromEmitter(stageId, emitter -> {
                    boolean bestEffort =
                            context.options().mergeStrategy() == MergeStrategy.BEST_EFFORT;

                    java.util.concurrent.LinkedBlockingQueue<Object> queue =
                            new java.util.concurrent.LinkedBlockingQueue<>();
                    java.util.concurrent.atomic.AtomicInteger remaining =
                            new java.util.concurrent.atomic.AtomicInteger(count);
                    java.util.concurrent.atomic.AtomicReference<Throwable> firstError =
                            bestEffort ? null
                                       : new java.util.concurrent.atomic.AtomicReference<>();
                    java.util.List<Throwable> errors =
                            bestEffort ? new java.util.concurrent.CopyOnWriteArrayList<>() : null;
                    final Object EOF = new Object();

                    for (Pipeline<? extends T> p : copy) {
                        startMergeContributor(p, queue, remaining, firstError, errors, EOF,
                                bestEffort);
                    }

                    while (true) {
                        Object item;
                        try {
                            item = queue.take();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        if (item == EOF) break;
                        T value = (T) item;
                        emitter.emit(value);
                    }

                    if (!bestEffort) {
                        Throwable err = firstError != null ? firstError.get() : null;
                        if (err instanceof RuntimeException re) throw re;
                        if (err != null) throw new RuntimeException("merge upstream failed", err);
                    } else if (errors != null && !errors.isEmpty()) {
                        RuntimeException combined = new RuntimeException(
                                "one or more merge upstreams failed (" + errors.size() + " error(s))");
                        errors.forEach(combined::addSuppressed);
                        throw combined;
                    }
                }, downstream, context),
                "Merge(" + count + ")");
    }

    private static <T, U extends T> void startMergeContributor(
            Pipeline<U> pipeline,
            java.util.concurrent.LinkedBlockingQueue<Object> queue,
            java.util.concurrent.atomic.AtomicInteger remaining,
            java.util.concurrent.atomic.AtomicReference<Throwable> firstError,
            java.util.List<Throwable> errors,
            Object eof,
            boolean bestEffort) {
        pipeline.sink(new Sink<U>() {
            @Override
            public void accept(U item) {
                try { queue.put(item); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            @Override
            public void onComplete() {
                if (remaining.decrementAndGet() == 0) putEof();
            }
            @Override
            public void onError(Throwable cause) {
                if (bestEffort) {
                    errors.add(cause);
                    if (remaining.decrementAndGet() == 0) putEof();
                } else {
                    if (firstError.compareAndSet(null, cause)) putEof();
                }
            }
            private void putEof() {
                try { queue.put(eof); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
        }).start();
    }

    /**
     * Assembles the given {@link PipelineDefinition} and returns a {@link PipelineHandle}
     * without starting the pipeline.
     *
     * <p>Equivalent to {@code definition.assemble()}. Use this method to obtain a handle
     * that can be configured with lifecycle callbacks and started at a controlled point.
     *
     * @param definition the pipeline definition to assemble; must not be {@code null}
     * @return a {@code PipelineHandle} representing the assembled, not-yet-started pipeline
     * @throws NullPointerException if {@code definition} is {@code null}
     */
    public static PipelineHandle wire(PipelineDefinition definition) {
        if (definition == null) throw new NullPointerException("definition must not be null");
        return definition.assemble();
    }

    /**
     * Assembles and immediately starts the given {@link PipelineDefinition}.
     *
     * <p>Equivalent to {@code Heddle.wire(definition).start()}. The returned handle
     * represents a running pipeline; call {@link PipelineHandle#awaitCompletion()} to
     * block until it finishes.
     *
     * @param definition the pipeline definition to assemble and start;
     *                   must not be {@code null}
     * @return a {@code PipelineHandle} representing the running pipeline
     * @throws NullPointerException if {@code definition} is {@code null}
     */
    public static PipelineHandle run(PipelineDefinition definition) {
        return wire(definition).start();
    }
}
