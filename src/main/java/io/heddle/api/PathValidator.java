package io.heddle.api;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Validates a {@link Path} before it is opened by a file source or sink.
 *
 * <p>Throw {@link SecurityException} to reject a path; return normally to allow it.
 * Validators are applied eagerly, before any file descriptor is opened.
 *
 * <p>{@code PathValidator} is a functional interface. Custom policies may be expressed
 * as lambdas:
 *
 * <pre>{@code
 * PathValidator noHidden = path -> {
 *     if (path.getFileName().toString().startsWith("."))
 *         throw new SecurityException("hidden files are not permitted: " + path);
 * };
 * }</pre>
 *
 * <p>The built-in factory {@link #withinDirectory(Path)} produces a validator that
 * prevents path-traversal attacks by rejecting any path that resolves outside a
 * declared base directory.
 *
 * @see io.heddle.Heddle#fromLines(Path, PathValidator)
 * @see io.heddle.Heddle#fromLines(Path, java.nio.charset.Charset, PathValidator)
 */
@FunctionalInterface
public interface PathValidator {

    /**
     * Validates the given path according to this validator's policy.
     *
     * @param path the path to validate
     * @throws SecurityException if this validator rejects the path
     */
    void validate(Path path);

    /**
     * Returns a validator that rejects any path not contained within the specified base
     * directory, guarding against path-traversal attacks.
     *
     * <p>For paths that already exist on disk, {@link Path#toRealPath()} is used to
     * resolve symbolic links before comparison. For paths that do not yet exist (for
     * example, write targets), {@link Path#toAbsolutePath() absolute normalization} is
     * used instead.
     *
     * <pre>{@code
     * PathValidator sandboxed = PathValidator.withinDirectory(Path.of("/data/uploads"));
     * Heddle.fromLines(userSuppliedPath, sandboxed)
     *       .forEach(System.out::println);
     * }</pre>
     *
     * @param base the directory within which all validated paths must reside;
     *             must not be {@code null}
     * @return a {@code PathValidator} that enforces containment within {@code base}
     * @throws NullPointerException if {@code base} is {@code null}
     */
    static PathValidator withinDirectory(Path base) {
        if (base == null) throw new NullPointerException("base must not be null");
        return path -> {
            try {
                Path resolved = Files.exists(path)
                        ? path.toRealPath()
                        : path.toAbsolutePath().normalize();
                Path realBase = base.toRealPath();
                if (!resolved.startsWith(realBase)) {
                    throw new SecurityException(
                            "path traversal denied: '" + path +
                            "' is outside base directory '" + base + "'");
                }
            } catch (IOException e) {
                throw new SecurityException(
                        "path validation failed for '" + path + "': " + e.getMessage(), e);
            }
        };
    }
}
