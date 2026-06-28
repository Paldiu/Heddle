package io.heddle.internal.file;

import io.heddle.api.Emitter;
import io.heddle.api.PathValidator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

/**
 * Push-style file source that streams lines via an {@link Emitter}.
 *
 * <p>Uses {@link FileChannel#map} to memory-map the file in segments, then decodes
 * each segment and scans for line terminators entirely in heap memory. After the
 * initial {@code mmap(2)} system call, no further OS I/O calls occur during the
 * read loop; the OS page-fault mechanism handles physical I/O transparently below
 * the JVM level. This means a virtual thread running this source never blocks on a
 * native file-read call, so it cannot pin its carrier platform thread.
 *
 * <p>Files larger than {@value #CHUNK_SIZE_BYTES} bytes are mapped in multiple
 * segments; incomplete lines at segment boundaries are carried forward as a
 * {@code String} tail and prepended to the next segment's decoded text.
 *
 * <p><b>Trust boundary:</b> the caller is responsible for validating that
 * {@code path} is within an expected directory. No path canonicalisation or
 * allow-listing is performed here; a path derived from untrusted input is a
 * path-traversal / arbitrary-read vulnerability.
 *
 * <p><b>Sensitive data:</b> lines are emitted as immutable {@code String} values.
 * These cannot be zeroed after use regardless of any {@link io.heddle.security.ClearHook}
 * registered on the pipeline. Do not use this source for secrets that must be
 * wiped from memory; use a {@code char[]}-based source instead.
 */
public final class FileSource implements Consumer<Emitter<String>> {

    static final long CHUNK_SIZE_BYTES = 8L * 1024 * 1024;   // 8 MiB per mapping segment

    private final Path    path;
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
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            if (fileSize == 0) return;

            CharsetDecoder decoder = charset.newDecoder();
            long offset = 0;
            String tail = "";

            while (offset < fileSize && !Thread.currentThread().isInterrupted()) {
                long chunkLen = Math.min(fileSize - offset, CHUNK_SIZE_BYTES);
                MappedByteBuffer segment = channel.map(FileChannel.MapMode.READ_ONLY, offset, chunkLen);
                offset += chunkLen;
                boolean lastChunk = (offset >= fileSize);

                String chunk = tail + decoder.decode(segment).toString();
                decoder.reset();
                tail = "";

                int start = 0;
                for (int i = 0; i < chunk.length(); i++) {
                    if (Thread.currentThread().isInterrupted()) return;
                    if (chunk.charAt(i) == '\n') {
                        int end = (i > 0 && chunk.charAt(i - 1) == '\r') ? i - 1 : i;
                        emitter.emit(chunk.substring(start, end));
                        start = i + 1;
                    }
                }

                if (start < chunk.length()) {
                    if (lastChunk) {
                        emitter.emit(chunk.substring(start));
                    } else {
                        tail = chunk.substring(start);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed reading " + path, e);
        }
    }

    public Path path()       { return path; }
    public Charset charset() { return charset; }
}
