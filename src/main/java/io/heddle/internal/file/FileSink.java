package io.heddle.internal.file;

import io.heddle.api.PathValidator;
import io.heddle.api.Sink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * Terminal sink that writes each item as a line to a file using a
 * configurable serializer. The channel is opened lazily on the first
 * {@link #accept} call and closed on {@link #onComplete()}.
 *
 * <p>Uses {@link AsynchronousFileChannel} for writes. Each write submits the
 * serialized bytes to the OS asynchronously and then calls {@code Future.get()},
 * which parks the calling virtual thread (unmounting its carrier) rather than
 * blocking a platform thread. This prevents the virtual thread from pinning its
 * carrier on file-write system calls, which is the failure mode of the former
 * {@link java.io.BufferedWriter} path under Loom.
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
 * <p><b>{@link StandardOpenOption#APPEND} is emulated:</b> when {@code APPEND}
 * is present in the options, it is replaced with {@code WRITE + CREATE} and
 * the initial write position is set to the file's current size, so appends
 * are position-tracked rather than OS-level append mode. This is necessary
 * because {@link AsynchronousFileChannel} does not natively support {@code APPEND}.
 *
 * <p><b>Trust boundary:</b> the caller is responsible for validating that
 * {@code path} is within an expected directory. No path canonicalisation or
 * allow-listing is performed here; arbitrary paths derived from untrusted
 * input are a path-traversal / arbitrary-write vulnerability.
 */
public final class FileSink<T> implements Sink<T> {

    private static final String LINE_SEP = System.lineSeparator();

    private final Path              path;
    private final Charset           charset;
    private final OpenOption[]      options;
    private final Function<T, String> serializer;
    private AsynchronousFileChannel channel;
    private long                    writePosition;

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
        ensureOpen();
        ByteBuffer buf = charset.encode(serializer.apply(item) + LINE_SEP);
        try {
            while (buf.hasRemaining()) {
                writePosition += channel.write(buf, writePosition).get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new UncheckedIOException("failed writing to " + path,
                    new IOException(e.getCause()));
        }
    }

    @Override
    public void onComplete() {
        closeChannel();
    }

    @Override
    public void onError(Throwable cause) {
        closeChannel();
    }

    private void ensureOpen() {
        if (channel != null) return;
        try {
            boolean append = false;
            List<OpenOption> effective = new ArrayList<>(options.length + 1);
            effective.add(StandardOpenOption.WRITE);
            for (OpenOption opt : options) {
                if (opt == StandardOpenOption.APPEND) {
                    append = true;
                    effective.add(StandardOpenOption.CREATE);
                } else if (opt != StandardOpenOption.WRITE) {
                    effective.add(opt);
                }
            }
            channel = AsynchronousFileChannel.open(path, effective.toArray(new OpenOption[0]));
            writePosition = append ? channel.size() : 0L;
        } catch (IOException e) {
            throw new UncheckedIOException("failed opening " + path, e);
        }
    }

    private void closeChannel() {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                throw new UncheckedIOException("failed closing " + path, e);
            } finally {
                channel = null;
                writePosition = 0L;
            }
        }
    }
}
