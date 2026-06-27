package io.heddle.api;

/**
 * Pull-style data provider for finite or self-terminating pipeline sources.
 *
 * <p>The pipeline source stage calls {@link #get()} on a dedicated virtual thread,
 * forwarding each non-{@code null} return value downstream, until {@code null} is
 * returned to signal end-of-stream. This pattern is well-suited to blocking data
 * sources such as JDBC result sets or socket readers: the virtual thread parks
 * during each blocking call without pinning a carrier thread.
 *
 * <p>For sources that push data reactively rather than being polled, use
 * {@link io.heddle.Heddle#from(java.util.function.Consumer)} with an {@link Emitter}
 * instead.
 *
 * <p>{@code Source} is a functional interface and is typically implemented as a
 * method reference or lambda:
 *
 * <pre>{@code
 * ResultSet rs = stmt.executeQuery("SELECT name FROM users");
 * Source<String> rows = () -> rs.next() ? rs.getString(1) : null;
 *
 * Heddle.from(rows)
 *       .map(String::toUpperCase)
 *       .forEach(System.out::println);
 * }</pre>
 *
 * @param <T> the type of items provided by this source
 * @see io.heddle.Heddle#from(java.util.function.Supplier)
 * @see Emitter
 */
@FunctionalInterface
public interface Source<T> {

    /**
     * Returns the next item from this source, or {@code null} to signal
     * end-of-stream.
     *
     * <p>Once this method returns {@code null}, the pipeline runtime treats the source
     * as exhausted and will not invoke this method again.
     *
     * @return the next item, or {@code null} to terminate the source
     */
    T get();
}
