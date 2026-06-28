package io.heddle.internal.file;

import io.heddle.api.Emitter;
import io.heddle.api.PathValidator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * Push-style file source that streams lines via an {@link Emitter}.
 * Uses a {@link BufferedReader} for efficient I/O; closes the reader on
 * completion or if the caller's thread is interrupted.
 *
 * <p><b>Trust boundary:</b> the caller is responsible for validating that
 * {@code path} is within an expected directory. No path canonicalisation or
 * allow-listing is performed here, a path derived from untrusted input is a
 * path-traversal / arbitrary-read vulnerability.
 *
 * <p><b>Sensitive data:</b> lines are emitted as immutable {@code String} values.
 * These cannot be zeroed after use regardless of any {@link io.heddle.security.ClearHook}
 * registered on the pipeline. Do not use this source for secrets that must be
 * wiped from memory; use a {@code char[]}-based source instead.
 */
public final class FileSource implements Consumer<Emitter<String>> {

    private final Path   path;
    private final Charset charset;

    public FileSource(Path path) {
        this(path, StandardCharsets.UTF_8);
    }

    public FileSource(Path path, Charset charset) {
        this.path    = path;
        this.charset = charset;
    }

    public FileSource(Path path, PathValidator validator) {
        this(validated(path, validator), StandardCharsets.UTF_8);
    }

    public FileSource(Path path, Charset charset, PathValidator validator) {
        this(validated(path, validator), charset);
    }

    private static Path validated(Path path, PathValidator validator) {
        if (validator != null) validator.validate(path);
        return path;
    }

    @Override
    public void accept(Emitter<String> emitter) {
        try (BufferedReader reader = Files.newBufferedReader(path, charset)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (Thread.currentThread().isInterrupted()) break;
                emitter.emit(line);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed reading " + path, e);
        }
    }

    public Path path()      { return path; }
    public Charset charset() { return charset; }
}
