# Heddle

A JVM 25+ data pipeline library built on Project Loom's virtual threads. Where Project Reactor composes async callbacks on a scheduler, Heddle puts each pipeline stage on its own virtual thread and lets it block freely. The carrier thread is released automatically, so blocking is cheap.

```java
Heddle.fromLines(Path.of("records.csv"))
      .map(CsvParser::parse)
      .filter(Record::isValid)
      .batch(100)
      .sink(db::insertBatch)
      .start();
```

> **Status:** Active development. The core engine, operator surface, file I/O, timing infrastructure, and in-band signal API are stable. A few advanced operators and interop bridges remain in progress.

---

## Requirements

- JDK 25 or later

No runtime dependencies.

---

## Sources

```java
// From any Iterable, varargs, or empty
Heddle.from(myList)
Heddle.of("a", "b", "c")
Heddle.empty()

// Supplier: return null to signal end-of-stream (wraps blocking cursors, JDBC ResultSets, etc.)
Heddle.from(() -> resultSet.next() ? resultSet.getString(1) : null)

// Push/emitter style for event-driven or infinite sources
Heddle.from((Emitter<Event> emit) -> eventBus.subscribe(emit::emit))

// Integer range [from, to)
Heddle.range(0, 1_000_000)

// Infinite generator
Heddle.generate(UUID::randomUUID)

// File convenience - uses memory-mapped I/O; virtual threads never pin a carrier thread
Heddle.fromLines(Path.of("data.txt"))                        // UTF-8 lines
Heddle.fromLines(Path.of("data.txt"), StandardCharsets.ISO_8859_1)
Heddle.fromBytes(inputStream, 4096)                          // byte[] chunks

// CompletableFuture as a single-item source
Heddle.from(myFuture)

// Fan-in from multiple iterables (sequential)
Heddle.merge(listA, listB, listC)
```

---

## Operators

### Transform

```java
.map(String::trim)
.mapAsync(this::callRemoteApi)       // bounded to Hardware.physicalCores() - CPU-bound
.mapAsync(this::callRemoteApi, 8)    // explicit concurrency cap
.flatMap(line -> Arrays.asList(line.split(",")))
.flatMap(this::expand, 4)            // concurrent expansion (I/O-bound)
```

### Filter

```java
.filter(s -> !s.isEmpty())
.filterType(String.class)            // narrow type; drops non-matching items
```

### Grouping

```java
.batch(100)                          // emit List<T> every 100 items; partial batch flushed at end
.window(Duration.ofSeconds(5))       // emit List<T> every 5 seconds regardless of count
```

### Flow control

```java
.buffer(512)                         // insert a larger inter-stage queue
.limit(1000)                         // pass at most n items downstream
.skip(10)                            // discard first n items
.throttle(500)                       // cap throughput at 500 items/sec
.timeout(Duration.ofMillis(200))     // fail item if stage takes longer than 200 ms
```

### Deduplication

```java
.distinct()                          // by identity/equals; unbounded
.distinct(Record::id)                // by key extractor
.distinct(10_000)                    // bounded: pass-through after 10k distinct keys
.distinct(10_000, true)              // bounded: fail pipeline when key set exhausted
```

### Side effects

```java
.peek(System.out::println)
.log()                               // logs each item via System.Logger
.log("after-parse")                  // log with a label prefix
.log(item -> auditLog.write(item))   // custom printer
```

### Fan-out

```java
// Synchronous broadcast to N consumers; each sees every item on the main VT
.broadcast(metricsConsumer, auditConsumer)

// Fire-and-forget branch on its own VT; errors in branch are isolated
.tee(errorLog::write)

// Full branch pipeline with separate operators
.tee(branch -> branch.filter(Event::isError).sink(errorLog))
```

### Backpressure

```java
.backpressure(BackpressurePolicy.DROP)       // silently discard when full
.backpressure(BackpressurePolicy.FAIL_FAST)  // fail pipeline on overflow
// default: BackpressurePolicy.BLOCK         // park producer until consumer catches up
```

### CompletableFuture integration

```java
// Unwrap a pipeline of CompletableFuture<R> into their values.
// Each VT parks on future.get(). No callbacks, no pinning.
Heddle.from(urlList)
      .map(url -> httpClient.sendAsync(url, ...))
      .awaitFutures()
      .map(HttpResponse::body)
      .forEach(this::process);
```

### Custom stages

```java
// Stateless: one-to-one or one-to-zero (filter shape) via lambda
Pipeline<String> p = source.stage((item, ctx) -> {
    String val = item.consume();
    if (!val.isBlank()) ctx.emit(val.trim());
});

// Stateful: implement Stage<I, O> directly
class BatchAggregator implements Stage<Event, Report> {
    private final List<Event> buf = new ArrayList<>();

    @Override
    public void process(Owned<Event> item, StageContext<Report> ctx) {
        buf.add(item.consume());
        if (buf.size() >= 1000) flush(ctx);
    }

    @Override
    public void flush(StageContext<Report> ctx) {
        if (!buf.isEmpty()) { ctx.emit(Report.of(buf)); buf.clear(); }
    }
}
```

---

## In-band signals

`Transfer.Signal<T, U>` is the third sealed variant alongside `Ready<T>` (data) and `Complete<T>` (end-of-stream). Signals carry a typed payload `U` that flows through the pipeline in-band alongside data items. They bypass the channel's backpressure drop policy so control messages are never silently discarded.

```java
// Emit a signal from any stage
ctx.emitSignal(new Watermark(Instant.now()));
ctx.emitSignal(FlushCommand.INSTANCE);

// Create a signal token directly for channel injection
Transfer.Signal<String, Watermark> token = Transfer.signal(new Watermark(...));
```

**Receiving signals in a stage:** override `Stage.onSignal`. The default implementation forwards the payload unchanged so stateless stages are transparent to signals.

```java
class WindowedAggregator implements Stage<Event, Report> {

    @Override
    public void process(Owned<Event> item, StageContext<Report> ctx) { ... }

    @Override
    public <U> void onSignal(U payload, StageContext<Report> ctx) {
        if (payload instanceof FlushCommand) {
            flushCurrentWindow(ctx);
            // do not forward: signal is consumed here
        } else {
            ctx.emitSignal(payload);   // pass unrecognised signals downstream
        }
    }
}
```

Signals reaching the terminal `SinkStage` are silently dropped. If a sink needs to react to signals, intercept them in a preceding stage.

---

## Terminals

**Blocking** (start the pipeline and wait for it to drain):

```java
List<T>              result  = pipeline.toList();
long                 n       = pipeline.count();
Optional<T>          first   = pipeline.first();
Optional<T>          reduced = pipeline.reduce(BinaryOperator);
<R>                  r       = pipeline.collect(Collectors.joining(","));
Map<K, V>            map     = pipeline.toMap(keyFn, valueFn);
Map<K, List<T>>      groups  = pipeline.groupBy(classifier);
Map<Boolean, List<T>> parts  = pipeline.partition(predicate);
                               pipeline.forEach(System.out::println);
                               pipeline.drain();        // run and discard output
```

**Non-blocking** (assemble the pipeline, get a handle, start separately):

```java
PipelineHandle handle = pipeline.sink(db::insert);
handle.start();
// ... do other work ...
handle.awaitCompletion();
if (handle.isFailed()) handle.failureCause().ifPresent(Throwable::printStackTrace);
```

**CompletableFuture bridge:**

```java
CompletableFuture<List<T>> future = pipeline.toFuture();
```

---

## Configuration

```java
pipeline
    .named("ingest-pipeline")                     // names the stage virtual threads
    .withOptions(PipelineOptions.defaults()
        .withBufferSize(256)                      // inter-stage queue depth (default: 64)
        .withDeadline(Duration.ofMinutes(5))      // whole-pipeline watchdog timeout
        .withCarrierPoolSize(4))                  // restrict Heddle's carrier pool to 4 threads
```

### Carrier pool size

By default the JVM's fork-join scheduler claims one carrier thread per available CPU. In embedded contexts (game engines, audio servers) where Heddle competes with a real-time loop, use `withCarrierPoolSize(n)` to restrict Heddle to a strict subset of cores:

```java
PipelineOptions opts = PipelineOptions.defaults()
    .withCarrierPoolSize(2);   // Heddle uses at most 2 carrier threads

Heddle.fromLines(path)
      .map(parse)
      .sink(store)
      .withOptions(opts)
      .start();
```

This sets the `jdk.virtualThread.scheduler.parallelism` system property before the first virtual thread is scheduled. It must be applied before any virtual threads are started to take effect.

---

## File I/O

`fromLines` and file sinks use Loom-safe I/O exclusively:

- **Reads** use `FileChannel.map()` / `MappedByteBuffer` in 8 MiB segments. After the initial `mmap(2)` call, no further OS I/O calls occur during the scan loop; the OS page-fault mechanism handles physical I/O below the JVM. Virtual threads never block on native file-read calls and cannot pin a carrier thread.
- **Writes** use `AsynchronousFileChannel`. Each write submits bytes to the OS asynchronously and then calls `Future.get()`, parking the calling virtual thread without consuming a platform thread.

The old `BufferedReader`/`BufferedWriter` path is gone. Both `FileSink` and `FileSource` are drop-in replacements with the same constructor signatures.

```java
// File sink: CREATE_NEW by default (prevents silent overwrites)
pipeline.sink(new FileSink<>(Path.of("out.txt")));

// Append mode
pipeline.sink(new FileSink<>(path, StandardCharsets.UTF_8, Object::toString,
    StandardOpenOption.CREATE, StandardOpenOption.APPEND));
```

---

## Off-heap buffers

`NativeBuffer` and `NativeBufferPool` provide flat, off-heap memory for stages that need cache-friendly data layout without GC pressure.

```java
// Single allocation
NativeBuffer buf = NativeBuffer.allocateInts(1024);
buf.setInt(0, 42);
int v = buf.getInt(0);
buf.close();   // release off-heap memory

// Copy from array
NativeBuffer buf = NativeBuffer.fromLongs(myLongArray);

// Pool: preallocate 16 buffers of 4096 bytes each; borrow/release pattern
try (NativeBufferPool pool = new NativeBufferPool(16, 4096)) {
    NativeBuffer buf = pool.borrow();
    try {
        // fill and use buf
    } finally {
        pool.release(buf);   // zeroed and returned to pool
    }
}
```

Buffers use a shared `Arena` so any virtual thread may read or write them across stage boundaries without synchronisation. Slices (`buf.slice(offset, length)`) share the parent's lifetime and must not be closed independently.

---

## Timing infrastructure

Throttle, timeout, and window operators are all driven by a single `HeddleWheelTimer` - one daemon platform thread running a 512-slot hashed wheel at 1 ms tick resolution. No per-item or per-stage timer threads are spawned, and no stage VT calls `Thread.sleep()`. Stage VTs park via `LockSupport.park()` / `unpark()` and are woken by the timer thread exactly when their deadline fires.

This eliminates the scheduling jitter that occurs when thousands of virtual threads each call `Thread.sleep()` independently under heavy Loom load, where the JVM scheduler cannot guarantee microsecond-level precision across a large VT population.

---

## Error handling

```java
.retry(3)                                          // retry a failing stage up to 3 times
.onError(new ErrorStrategy.Skip())                 // drop items that cause exceptions
.onError(new ErrorStrategy.Stop())                 // halt the pipeline on first error (default)
.onError(ErrorStrategy.deadLetterTo(dead::add))    // route failing items to a consumer
```

Combine with `timeout` to give a per-item budget and dead-letter items that take too long:

```java
pipeline.map(this::callSlowApi)
        .timeout(Duration.ofMillis(500))
        .onError(ErrorStrategy.deadLetterTo(timedOut::add))
        .filter(Objects::nonNull);
```

---

## Class-based pipelines

For long-lived or component-wired pipelines, implement `PipelineDefinition` and bind stage logic with method references:

```java
public class IngestPipeline implements PipelineDefinition {

    @Override
    public PipelineHandle assemble() {
        return Heddle.from(this::source)
                .map(this::parse)
                .filter(this::validate)
                .batch(100)
                .sink(this::persist);
    }

    private void source(Emitter<String> emit) { /* push items */ }
    private Record parse(String line)          { return CsvParser.parse(line); }
    private boolean validate(Record r)         { return r.isValid(); }
    private void persist(List<Record> batch)   { db.insertBatch(batch); }
}

// Wire without starting:
PipelineHandle handle = Heddle.wire(new IngestPipeline());
handle.start();

// Wire and start immediately:
PipelineHandle handle = Heddle.run(new IngestPipeline());
handle.awaitCompletion();
```

`PipelineDefinition` is a functional interface, so lambdas work too:

```java
PipelineHandle handle = Heddle.run(() ->
    Heddle.fromLines(Path.of("data.txt"))
          .map(String::trim)
          .filter(s -> !s.isEmpty())
          .sink(System.out::println));
```

---

## Transactional stages

Extend `TransactionStage` for stages that need commit/rollback semantics:

```java
public class JdbcBatchStage extends TransactionStage<List<Record>> {
    @Override public void begin()                        { conn.setAutoCommit(false); }
    @Override public void process(List<Record> batch)    { stmt.executeBatch(batch); }
    @Override public void commit()                       { conn.commit(); }
    @Override public void rollback(Throwable cause)      { conn.rollback(); }
}
```

---

## Sensitive data

`SensitiveDataHandler` and `ClearHook` zero out sensitive fields when a stage completes or the pipeline is cancelled:

```java
pipeline.map(this::decryptPayload)
        // register a ClearHook on the relevant Owned<T> to zero the byte[] on release
```

---

## Pipeline introspection

```java
// Human-readable stage graph
System.out.println(pipeline.describe());
// Source: File[data.csv]
// Stage 0: MapProcessor
// Stage 1: FilterProcessor
// Stage 2: BatchProcessor(100)
```

---

## How it works

```
Source VT → [HeddleChannel] → Stage-1 VT → [HeddleChannel] → … → Sink VT
```

Each arrow is a bounded `ArrayBlockingQueue`. Virtual threads block on `put()` and `take()`; the JVM unmounts them from their carrier thread while parked, so 10 000 in-flight items occupy 10 000 virtual threads but only a handful of platform threads. No reactive operators, no schedulers, no callbacks.

The internal token type is `Transfer<T>`, a sealed interface with three variants:

- `Transfer.Ready<T>` - a live data item
- `Transfer.Complete<T>` - end-of-stream sentinel (singleton, zero allocation)
- `Transfer.Signal<T, U>` - in-band control message with typed payload `U`

`AdmissionController` uses striped semaphores (one `Semaphore(1)` per concurrency slot) so that concurrent `mapAsync`/`flatMap` acquire/release operations never contend on a single AQS field. Each acquire returns a stripe index that is passed back to release.

Failure is out-of-band: stages call `PipelineContext.signalFailure()` rather than routing an error token down the data path, which would trap it behind a backed-up queue.

---

## Coming soon

| Feature | Notes |
|---|---|
| `retry(n, backoff)` | Retry with configurable delay between attempts |
| `merge(Pipeline...)` | True concurrent fan-in from multiple pipelines in the fluent API |
| `onError(Consumer)` | Inline error handler without a full `ErrorStrategy` |
| `java.util.concurrent.Flow` adapters | Bridge to Reactor, Mutiny, and other RS libraries |
| `PipelineHandle.stats()` | Per-stage queue depth, drop counts, and throughput metrics |
