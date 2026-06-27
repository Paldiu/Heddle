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

> **Status:** Active development. The core engine is stable and the operator surface is expanding. A handful of operators (`window`, `throttle`, `timeout`, `retry` with backoff) are in progress.

---

## Why

Project Reactor solves the "don't block the event loop" problem by turning every operation into a callback. The model is powerful but imposes a cognitive cost: you can't use normal Java control flow, checked exceptions are awkward, and stack traces are nearly unreadable.

Loom makes that trade-off unnecessary. Virtual threads are cheap enough to block; parking one unmounts its carrier thread, returning it to the pool. Heddle takes this at face value:

- Every stage runs on its own virtual thread.
- Stages hand items to the next stage through a bounded `ArrayBlockingQueue`. If the consumer is slow, the producer parks. If the consumer is fast, it parks waiting for the next item.
- User code is ordinary Java: `try/catch`, loops, JDBC calls, whatever.

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

// File convenience
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
.mapAsync(this::callRemoteApi, 8)   // up to 8 concurrent virtual threads
.flatMap(line -> Arrays.asList(line.split(",")))
.flatMap(this::expand, 4)           // concurrent expansion
```

### Filter

```java
.filter(s -> !s.isEmpty())
.filterType(String.class)           // narrow type; drops non-matching items
```

### Grouping

```java
.batch(100)                         // emit List<T> every 100 items; partial batch flushed at end
```

### Flow control

```java
.buffer(512)                        // insert a larger inter-stage queue
.limit(1000)                        // pass at most n items downstream
.skip(10)                           // discard first n items
```

### Deduplication

```java
.distinct()                         // by identity/equals
.distinct(Record::id)               // by key extractor
```

### Side effects

```java
.peek(System.out::println)
.log()                              // logs each item via System.Logger
.log("after-parse")                 // log with a label prefix
.log(item -> auditLog.write(item))  // custom printer
```

### Fan-out

```java
.broadcast(metricsConsumer, auditConsumer)   // copy each item to N consumers
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

---

## Terminals

**Blocking** (start the pipeline and wait for it to drain):

```java
List<T>      result  = pipeline.toList();
long         n       = pipeline.count();
Optional<T>  first   = pipeline.first();
Optional<T>  reduced = pipeline.reduce(BinaryOperator);
<R>          r       = pipeline.collect(Collectors.joining(","));
             pipeline.forEach(System.out::println);
             pipeline.drain();                        // run and discard output
```

**Non-blocking** (assemble the pipeline, get a handle, start separately):

```java
PipelineHandle handle = pipeline.sink(db::insert);
handle.start();
// ... do other work ...
handle.awaitCompletion();
if (handle.isFailed()) handle.cause().printStackTrace();
```

**CompletableFuture bridge:**

```java
CompletableFuture<List<T>> future = pipeline.toFuture();
```

---

## Configuration

```java
pipeline
    .named("ingest-pipeline")                 // names the stage virtual threads
    .withOptions(PipelineOptions.defaults()
        .withBufferSize(256)                  // inter-stage queue depth (default: 64)
        .withDeadline(Duration.ofMinutes(5))) // whole-pipeline timeout
```

---

## Backpressure

The default policy is `BackpressurePolicy.BLOCK`: a fast producer parks when the downstream queue is full. Alternatives:

```java
BackpressurePolicy.DROP       // silently discard when full; no blocking
BackpressurePolicy.FAIL_FAST  // signal pipeline failure immediately on overflow
```

Per-stage backpressure configuration via the fluent API is on the roadmap.

---

## Class-based pipelines

For long-lived or component-wired pipelines, implement `PipelineDefinition` and bind stage logic with method references. The class encapsulates its own methods; no reflection or annotation scanning occurs.

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

    private void source(Emitter<String> emit) {
        Files.lines(Path.of("input.txt")).forEach(emit::emit);
    }

    private Record parse(String line) {
        return CsvParser.parse(line);
    }

    private boolean validate(Record r) {
        return r.isValid();
    }

    private void persist(List<Record> batch) {
        db.insertBatch(batch);
    }
}

// Wire without starting (useful for testing or deferred start):
PipelineHandle handle = Heddle.wire(new IngestPipeline());
handle.start();

// Or wire and start immediately:
PipelineHandle handle = Heddle.run(new IngestPipeline());
handle.awaitCompletion();
```

Since `PipelineDefinition` is a functional interface, you can also pass a lambda directly:

```java
PipelineHandle handle = Heddle.run(() ->
    Heddle.fromLines(Path.of("data.txt"))
          .map(String::trim)
          .filter(s -> !s.isEmpty())
          .sink(System.out::println));
```

---

## Error handling

```java
.retry(3)                           // retry a failing stage up to 3 times
.onError(new ErrorStrategy.Skip())  // drop items that cause exceptions (wiring in progress)
.onError(new ErrorStrategy.Stop())  // halt the pipeline on first error (default)
```

The `DeadLetter` strategy routes failing items to a consumer instead of halting:

```java
List<Record> dead = new ArrayList<>();
.onError(ErrorStrategy.deadLetterTo(dead::add))   // coming soon
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

## How it works

```
Source VT ──→ [HeddleChannel] ──→ Stage-1 VT ──→ [HeddleChannel] ──→ … ──→ Sink VT
```

Each arrow is a bounded `ArrayBlockingQueue`. Virtual threads block on `put()` and `take()`; the JVM unmounts them from their carrier thread while parked, so 10 000 in-flight items occupy 10 000 virtual threads but only a handful of platform threads. No reactive operators, no schedulers, no callbacks.

---

## Coming soon

| Feature | Notes |
|---|---|
| `window(Duration)` | Time-based windowing with a timer virtual thread |
| `throttle(int)` | Rate limiting via `Thread.sleep` on the stage VT |
| `timeout(Duration)` | Per-item watchdog on a child virtual thread |
| `retry(n, backoff)` | Retry with configurable delay |
| `merge(Pipeline...)` | True concurrent fan-in; requires DAG assembler |
| `onError(Consumer)` | Inline error handler without full `ErrorStrategy` |
| Per-stage backpressure policy | Fluent `.backpressure(policy)` operator |
| `java.util.concurrent.Flow` adapters | Bridge to Reactor, Mutiny, and other RS libraries |
| `PipelineHandle.stats()` | Per-stage queue depth, drop counts, throughput |
