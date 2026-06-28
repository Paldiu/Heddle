package io.heddle.internal.file;

import io.heddle.api.PathValidator;
import io.heddle.api.Sink;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Function;

/**
 * Terminal sink that writes each item as a line to a file using a
 * configurable serializer. The writer is opened lazily on the first
 * {@link #accept} call and flushed and closed on {@link #onComplete()}.
 *
 * <p><b>Default open mode is {@link StandardOpenOption#CREATE_NEW}.</b>
 * This prevents silent data-loss by refusing to overwrite an existing file.
 * Opt into truncation explicitly via the constructor that accepts
 * {@code OpenOption...}:
 * <pre>{@code
 *   new FileSink<>(path, StandardCharsets.UTF_8, Object::toString,
 *       StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
 * }</pre>
 *
 * <p><b>Trust boundary:</b> the caller is responsible for validating that
 * {@code path} is within an expected directory. No path canonicalisation or
 * allow-listing is performed here, arbitrary paths derived from untrusted
 * input are a path-traversal / arbitrary-write vulnerability.
 */
public final class FileSink<T> implements Sink<T> {

    private final Path              path;
    private final Charset           charset;
    private final OpenOption[]      options;
    private final Function<T, String> serializer;
    private BufferedWriter          writer;

    /** Convenience: UTF-8, CREATE_NEW, {@code toString()} serializer. */
    public FileSink(Path path) {
        this(path, StandardCharsets.UTF_8, Object::toString, StandardOpenOption.CREATE_NEW);
    }

    /** Convenience with path validation; UTF-8, CREATE_NEW, {@code toString()} serializer. */
    public FileSink(Path path, PathValidator validator) {
        this(validated(path, validator), StandardCharsets.UTF_8, Object::toString,
                StandardOpenOption.CREATE_NEW);
    }

    /** Full control over charset, serializer, and open options. */
    public FileSink(Path path, Charset charset, Function<T, String> serializer, OpenOption... options) {
        if (path == null)       throw new NullPointerException("path must not be null");
        if (charset == null)    throw new NullPointerException("charset must not be null");
        if (serializer == null) throw new NullPointerException("serializer must not be null");
        this.path       = path;
        this.charset    = charset;
        this.serializer = serializer;
        this.options    = options.length == 0
                ? new OpenOption[]{StandardOpenOption.CREATE_NEW}
                : options.clone();
    }

    /** Full control with path validation; validator runs before any I/O. */
    public FileSink(Path path, Charset charset, Function<T, String> serializer,
                    PathValidator validator, OpenOption... options) {
        this(validated(path, validator), charset, serializer, options);
    }

    private static Path validated(Path path, PathValidator validator) {
        if (validator != null) validator.validate(path);
        return path;
    }

    @Override
    public void accept(T item) {
        if (writer == null) {
            try {
                writer = Files.newBufferedWriter(path, charset, options);
            } catch (IOException e) {
                throw new UncheckedIOException("failed opening " + path, e);
            }
        }
        try {
            writer.write(serializer.apply(item));
            writer.newLine();
        } catch (IOException e) {
            throw new UncheckedIOException("failed writing to " + path, e);
        }
    }

    @Override
    public void onComplete() {
        closeWriter();
    }

    @Override
    public void onError(Throwable cause) {
        closeWriter();
    }

    private void closeWriter() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                throw new UncheckedIOException("failed closing " + path, e);
            } finally {
                writer = null;
            }
        }
    }
}
