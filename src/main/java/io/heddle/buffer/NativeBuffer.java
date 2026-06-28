package io.heddle.buffer;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A contiguous, off-heap memory buffer for use as a pipeline item.
 *
 * <p>Standard Java object graphs force the CPU to chase heap pointers across DRAM
 * on every field access. When a virtual thread picks up a processing task, the
 * physical core's L1/L2 caches must be warm for throughput to be good. A
 * {@code NativeBuffer} stores data in a single {@link MemorySegment}, one flat
 * allocation, so the JVM can hand the hardware prefetcher a predictable stride
 * and avoid the memory-bus saturation that pointer-heavy object graphs cause under
 * high concurrency.
 *
 * <p><b>Ownership model.</b> A buffer created by one of the {@code allocate*} or
 * {@code from*} factory methods owns its {@link Arena} and must be closed exactly
 * once after use. Buffers returned by {@link #slice} share the parent's arena and
 * must <em>not</em> be closed independently, closing a slice does nothing.
 *
 * <p><b>Thread safety.</b> Buffers use a {@linkplain Arena#ofShared() shared arena}
 * so that any virtual thread, including those on different carrier threads, may
 * read and write the buffer without restriction. This is the correct default for
 * pipeline items that cross stage boundaries.
 *
 * <p>Common usage: pack tightly-related fields into a single {@code int} or {@code long}
 * using bit-masking, then store those packed values in a {@code NativeBuffer} of ints
 * or longs. Downstream stages read the buffer sequentially, keeping the cache line hot.
 */
public final class NativeBuffer implements AutoCloseable {

    private final MemorySegment segment;
    private final Arena arena;

    private NativeBuffer(MemorySegment segment, Arena arena) {
        this.segment = segment;
        this.arena   = arena;
    }

    // -------------------------------------------------------------------------
    // Factory methods
    // -------------------------------------------------------------------------

    /**
     * Allocates a new buffer of exactly {@code byteSize} bytes.
     *
     * @param byteSize number of bytes to allocate; must be positive
     * @return a new buffer that the caller must {@link #close} when done
     */
    public static NativeBuffer allocateBytes(long byteSize) {
        if (byteSize <= 0) throw new IllegalArgumentException("byteSize must be positive");
        Arena arena = Arena.ofShared();
        return new NativeBuffer(arena.allocate(byteSize), arena);
    }

    /**
     * Allocates a new buffer sized for {@code count} {@code int} values.
     *
     * @param count number of ints; must be positive
     * @return a new buffer that the caller must {@link #close} when done
     */
    public static NativeBuffer allocateInts(int count) {
        if (count <= 0) throw new IllegalArgumentException("count must be positive");
        return allocateBytes((long) count * Integer.BYTES);
    }

    /**
     * Allocates a new buffer sized for {@code count} {@code long} values.
     *
     * @param count number of longs; must be positive
     * @return a new buffer that the caller must {@link #close} when done
     */
    public static NativeBuffer allocateLongs(int count) {
        if (count <= 0) throw new IllegalArgumentException("count must be positive");
        return allocateBytes((long) count * Long.BYTES);
    }

    /**
     * Copies the given int array into a new off-heap buffer.
     *
     * @param data source array; must not be {@code null} or empty
     * @return a new buffer containing the same values, that the caller must {@link #close}
     */
    public static NativeBuffer fromInts(int[] data) {
        if (data == null || data.length == 0)
            throw new IllegalArgumentException("data must not be null or empty");
        NativeBuffer buf = allocateInts(data.length);
        MemorySegment.copy(data, 0, buf.segment, ValueLayout.JAVA_INT, 0, data.length);
        return buf;
    }

    /**
     * Copies the given long array into a new off-heap buffer.
     *
     * @param data source array; must not be {@code null} or empty
     * @return a new buffer containing the same values, that the caller must {@link #close}
     */
    public static NativeBuffer fromLongs(long[] data) {
        if (data == null || data.length == 0)
            throw new IllegalArgumentException("data must not be null or empty");
        NativeBuffer buf = allocateLongs(data.length);
        MemorySegment.copy(data, 0, buf.segment, ValueLayout.JAVA_LONG, 0, data.length);
        return buf;
    }

    /**
     * Copies the given byte array into a new off-heap buffer.
     *
     * @param data source array; must not be {@code null} or empty
     * @return a new buffer containing the same bytes, that the caller must {@link #close}
     */
    public static NativeBuffer fromBytes(byte[] data) {
        if (data == null || data.length == 0)
            throw new IllegalArgumentException("data must not be null or empty");
        NativeBuffer buf = allocateBytes(data.length);
        MemorySegment.copy(data, 0, buf.segment, ValueLayout.JAVA_BYTE, 0, data.length);
        return buf;
    }

    // -------------------------------------------------------------------------
    // Typed accessors
    // -------------------------------------------------------------------------

    /**
     * Reads the {@code int} at element position {@code index}.
     *
     * @param index zero-based element index (not byte offset)
     */
    public int getInt(long index) {
        return segment.getAtIndex(ValueLayout.JAVA_INT, index);
    }

    /**
     * Writes {@code value} at element position {@code index}.
     *
     * @param index zero-based element index (not byte offset)
     */
    public void setInt(long index, int value) {
        segment.setAtIndex(ValueLayout.JAVA_INT, index, value);
    }

    /**
     * Reads the {@code long} at element position {@code index}.
     *
     * @param index zero-based element index (not byte offset)
     */
    public long getLong(long index) {
        return segment.getAtIndex(ValueLayout.JAVA_LONG, index);
    }

    /**
     * Writes {@code value} at element position {@code index}.
     *
     * @param index zero-based element index (not byte offset)
     */
    public void setLong(long index, long value) {
        segment.setAtIndex(ValueLayout.JAVA_LONG, index, value);
    }

    /**
     * Reads the {@code byte} at byte offset {@code offset}.
     */
    public byte getByte(long offset) {
        return segment.get(ValueLayout.JAVA_BYTE, offset);
    }

    /**
     * Writes {@code value} at byte offset {@code offset}.
     */
    public void setByte(long offset, byte value) {
        segment.set(ValueLayout.JAVA_BYTE, offset, value);
    }

    // -------------------------------------------------------------------------
    // Slicing and info
    // -------------------------------------------------------------------------

    /**
     * Returns a view of a sub-range of this buffer.
     *
     * <p>The slice shares the parent's memory and lifetime. Do not call
     * {@link #close()} on a slice, close the owning buffer instead.
     *
     * @param offsetBytes byte offset from the start of this buffer; must be non-negative
     * @param lengthBytes number of bytes in the slice; must be positive
     */
    public NativeBuffer slice(long offsetBytes, long lengthBytes) {
        return new NativeBuffer(segment.asSlice(offsetBytes, lengthBytes), null);
    }

    /**
     * Returns the total size of this buffer in bytes.
     */
    public long byteSize() {
        return segment.byteSize();
    }

    /**
     * Returns how many {@code int} values fit in this buffer.
     */
    public long intCount() {
        return segment.byteSize() / Integer.BYTES;
    }

    /**
     * Returns how many {@code long} values fit in this buffer.
     */
    public long longCount() {
        return segment.byteSize() / Long.BYTES;
    }

    /**
     * Copies all int values into a new on-heap array.
     *
     * <p>Avoid in hot paths. Use for testing or for sinks that require an on-heap array.
     */
    public int[] toIntArray() {
        int count = (int) intCount();
        int[] result = new int[count];
        MemorySegment.copy(segment, ValueLayout.JAVA_INT, 0, result, 0, count);
        return result;
    }

    /**
     * Copies all long values into a new on-heap array.
     *
     * <p>Avoid in hot paths. Use for testing or for sinks that require an on-heap array.
     */
    public long[] toLongArray() {
        long count = longCount();
        long[] result = new long[(int) count];
        MemorySegment.copy(segment, ValueLayout.JAVA_LONG, 0, result, 0, (int) count);
        return result;
    }

    /**
     * Returns the underlying {@link MemorySegment} for use with low-level APIs
     * such as the Vector API or native function calls.
     */
    public MemorySegment segment() {
        return segment;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Zeroes every byte in this buffer.
     *
     * <p>The JDK implements this via {@code Unsafe.setMemory}, which the JIT
     * vectorises to a {@code memset}-equivalent. Call before returning a borrowed
     * buffer to a {@link NativeBufferPool}.
     */
    public void zero() {
        segment.fill((byte) 0);
    }

    /**
     * Releases the off-heap memory backing this buffer.
     *
     * <p>Must be called exactly once on the owning buffer. Calling {@code close()}
     * on a slice produced by {@link #slice} has no effect.
     */
    @Override
    public void close() {
        if (arena != null) arena.close();
    }
}
