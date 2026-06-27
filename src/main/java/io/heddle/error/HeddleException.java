package io.heddle.error;

/**
 * Base unchecked exception for errors reported by the Heddle pipeline runtime.
 *
 * <p>All exception types defined in the {@code io.heddle.error} package extend this
 * class, enabling callers to catch the full hierarchy of Heddle-specific failures
 * with a single {@code catch (HeddleException e)} clause.
 *
 * @see TimeoutException
 */
public class HeddleException extends RuntimeException {

    /**
     * Constructs a {@code HeddleException} with the given detail message.
     *
     * @param message the detail message
     */
    public HeddleException(String message) {
        super(message);
    }

    /**
     * Constructs a {@code HeddleException} with the given detail message and cause.
     *
     * @param message the detail message
     * @param cause   the exception that triggered this failure
     */
    public HeddleException(String message, Throwable cause) {
        super(message, cause);
    }
}
