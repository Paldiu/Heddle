package io.heddle.buffer;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * Fixed pool of preallocated {@link NativeBuffer} instances.
 *
 * <p>Each buffer is allocated off-heap at construction time and held in a
 * bounded {@link ArrayBlockingQueue}. Borrowing a buffer reuses the same
 * off-heap region, keeping the underlying virtual-to-physical page mappings
 * warm in the TLB and avoiding the syscall overhead of new
 * {@link java.lang.foreign.Arena} allocations per pipeline item.
 *
 * <p>{@link #borrow()} blocks until a buffer is available, parking the
 * calling virtual thread without consuming a platform thread. Callers must
 * call {@link #release(NativeBuffer)} when done; the buffer is zeroed and
 * returned to the pool.
 *
 * <p>The pool owns every buffer it allocates. Call {@link #close()} exactly
 * once when the pipeline shuts down to release all off-heap memory.
 *
 * <pre>{@code
 * try (NativeBufferPool pool = new NativeBufferPool(16, 4096)) {
 *     NativeBuffer buf = pool.borrow();
 *     try {
 *         // use buf
 *     } finally {
 *         pool.release(buf);
 *     }
 * }
 * }</pre>
 */
public final class NativeBufferPool implements AutoCloseable {

    private final ArrayBlockingQueue<NativeBuffer> pool;
    private final NativeBuffer[] owned;

    /**
     * Preallocates {@code capacity} buffers, each of {@code bufferSizeBytes} bytes.
     *
     * @param capacity        number of buffers in the pool; must be positive
     * @param bufferSizeBytes size of each buffer in bytes; must be positive
     */
    public NativeBufferPool(int capacity, long bufferSizeBytes) {
        if (capacity <= 0)        throw new IllegalArgumentException("capacity must be positive");
        if (bufferSizeBytes <= 0) throw new IllegalArgumentException("bufferSizeBytes must be positive");
        this.owned = new NativeBuffer[capacity];
        this.pool  = new ArrayBlockingQueue<>(capacity);
        for (int i = 0; i < capacity; i++) {
            NativeBuffer buf = NativeBuffer.allocateBytes(bufferSizeBytes);
            owned[i] = buf;
            pool.offer(buf);
        }
    }

    /**
     * Borrows a buffer from the pool, blocking until one is available.
     *
     * <p>The returned buffer is zeroed. The calling virtual thread parks
     * without consuming a platform thread if the pool is empty.
     *
     * @return a zeroed buffer; must be returned via {@link #release(NativeBuffer)}
     * @throws InterruptedException if interrupted while waiting
     */
    public NativeBuffer borrow() throws InterruptedException {
        return pool.take();
    }

    /**
     * Borrows a buffer without blocking, returning {@code null} if none are available.
     */
    public NativeBuffer tryBorrow() {
        return pool.poll();
    }

    /**
     * Returns a borrowed buffer to the pool after zeroing its contents.
     *
     * @param buffer a buffer previously obtained from this pool
     */
    public void release(NativeBuffer buffer) {
        buffer.zero();
        pool.offer(buffer);
    }

    /**
     * Returns the number of buffers currently available for borrowing.
     */
    public int available() {
        return pool.size();
    }

    /**
     * Releases all off-heap memory owned by this pool.
     *
     * <p>Must be called exactly once after the pool is no longer needed.
     * Accessing a borrowed buffer after closing the pool that owns it has
     * undefined behaviour.
     */
    @Override
    public void close() {
        for (NativeBuffer buf : owned) {
            if (buf != null) buf.close();
        }
    }
}
