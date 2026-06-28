package io.heddle.concurrent;

import io.heddle.error.HeddleException;
import io.heddle.util.ThreadUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tracks child virtual threads spawned by a flatMap or mapAsync stage so that
 * an out-of-band interrupt of the parent can cascade to all in-flight children.
 *
 * <p><b>Lock hierarchy:</b> ChildThreadTracker (ReentrantLock) sits <em>below</em>
 * {@link AdmissionController} and <em>above</em> PipelineContext (lock-free) in the
 * lock order. Never hold this lock while trying to acquire an AdmissionController
 * permit; that ordering would deadlock against a thread that holds the permit and
 * is waiting to track a child.
 *
 * <p><b>Why {@link ReentrantLock} over {@code synchronized}:</b> {@code synchronized}
 * blocks indefinitely with no diagnostic when the lock is not released (e.g. a thread
 * dies while holding it). {@link ReentrantLock#tryLock} with a timeout converts a
 * silent hang into a detectable {@link HeddleException}, making deadlocks observable.
 *
 * <p><b>Track-before-start invariant:</b> always use {@link #trackAndStart} rather
 * than {@code Thread.start()} + {@link #track}. A child that completes before its
 * parent calls {@link #track} would call {@link #untrack} on a thread not yet in the
 * list, leaving a phantom entry after {@link #track} runs. {@link #trackAndStart}
 * closes this race by registering the unstarted thread before it can complete.
 */
public final class ChildThreadTracker {

    static final long DEFAULT_LOCK_TIMEOUT_MS = 5_000;

    private final List<Thread> children = new ArrayList<>();
    private final ReentrantLock lock;
    private final long lockTimeoutMs;

    public ChildThreadTracker() {
        this(DEFAULT_LOCK_TIMEOUT_MS);
    }

    public ChildThreadTracker(long lockTimeoutMs) {
        if (lockTimeoutMs <= 0) throw new IllegalArgumentException("lockTimeoutMs must be positive");
        this.lock           = new ReentrantLock();
        this.lockTimeoutMs  = lockTimeoutMs;
    }

    /**
     * Create an unstarted virtual thread, register it, then start it.
     * This closes the race where a fast-completing child calls {@link #untrack}
     * before the parent calls {@link #track}.
     */
    public Thread trackAndStart(String name, Runnable task) {
        Thread child = Thread.ofVirtual().name(name).unstarted(task);
        track(child);
        child.start();
        return child;
    }

    public void track(Thread child) {
        acquireLock();
        try {
            children.add(child);
        } finally {
            lock.unlock();
        }
    }

    public void untrack(Thread child) {
        acquireLock();
        try {
            children.remove(child);
        } finally {
            lock.unlock();
        }
    }

    public void interruptAll() {
        acquireLock();
        try {
            children.forEach(Thread::interrupt);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Block until every tracked child has finished.
     *
     * <p>Takes a snapshot under the lock so concurrent {@link #track}/{@link #untrack}
     * calls cannot deadlock against the join. A child added after the snapshot is not
     * joined in this call, but that can only happen if the caller is processing the
     * next item before this call returns, which the single-threaded stage model prevents.
     */
    public void awaitAll() {
        List<Thread> snapshot;
        acquireLock();
        try {
            snapshot = List.copyOf(children);
        } finally {
            lock.unlock();
        }
        ThreadUtils.joinAll(snapshot.toArray(new Thread[0]));
    }

    public int activeCount() {
        acquireLock();
        try {
            return children.size();
        } finally {
            lock.unlock();
        }
    }

    private void acquireLock() {
        try {
            if (!lock.tryLock(lockTimeoutMs, TimeUnit.MILLISECONDS)) {
                throw new HeddleException(
                        "ChildThreadTracker lock timed out after " + lockTimeoutMs +
                        "ms; possible deadlock in stage child management");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HeddleException("ChildThreadTracker lock acquire interrupted", e);
        }
    }
}
