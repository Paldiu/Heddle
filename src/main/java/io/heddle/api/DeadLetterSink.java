package io.heddle.api;

/**
 * Specialized sink that receives items rejected by a pipeline stage, together with
 * the exception that caused the rejection.
 *
 * <p>A {@code DeadLetterSink} is attached to a stage via
 * {@link io.heddle.error.ErrorStrategy#deadLetterTo(DeadLetterSink)}, which is then
 * applied with {@link io.heddle.Pipeline#onError(io.heddle.error.ErrorStrategy)}. When
 * a stage throws an exception for a particular item, that item and its cause are routed
 * here instead of propagating as a pipeline failure, and processing continues with the
 * next item normally.
 *
 * <pre>{@code
 * List<FailedRecord> failures = new CopyOnWriteArrayList<>();
 *
 * Heddle.fromLines(path)
 *       .map(CsvParser::parse)
 *       .onError(ErrorStrategy.deadLetterTo(
 *           (line, ex) -> failures.add(new FailedRecord(line, ex))))
 *       .filter(Record::isValid)
 *       .forEach(db::insert);
 * }</pre>
 *
 * @param <T> the item type of the originating stage
 * @see io.heddle.error.ErrorStrategy#deadLetterTo(DeadLetterSink)
 * @see io.heddle.error.ErrorStrategy.DeadLetter
 */
@FunctionalInterface
public interface DeadLetterSink<T> {

    /**
     * Accepts an item that could not be processed by a pipeline stage, together with
     * the exception that caused the failure.
     *
     * <p>Implementations should not throw exceptions. Any unchecked exception thrown
     * from this method will be treated as an unhandled pipeline failure.
     *
     * @param item  the item that failed processing; the exact type corresponds to
     *              the input type of the stage that rejected it
     * @param cause the exception thrown by the failing stage
     */
    void accept(T item, Throwable cause);
}
