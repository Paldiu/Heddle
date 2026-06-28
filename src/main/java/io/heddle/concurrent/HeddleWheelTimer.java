package io.heddle.concurrent;

import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

/**
 * Singleton hashed wheel timer backed by a single dedicated daemon platform thread.
 *
 * <p>All time-based pipeline events — throttle delays, timeout evictions, window ticks,
 * and pipeline deadline watchdogs — are routed through this shared instance. This avoids
 * the per-virtual-thread sleep jitter documented in recent Loom releases when large
 * numbers of VTs each call {@link Thread#sleep} independently: under microsecond
 * throughput, the JVM timer infrastructure can experience extreme scheduling lag.
 *
 * <h2>Algorithm</h2>
 * A 512-slot wheel advances at 1ms per tick. Each slot is a lock-free CAS stack of
 * {@link Node}s. When a node's slot fires, its {@code rounds} counter is checked:
 * if positive the node is re-queued with {@code rounds - 1}; if zero the task fires.
 * A delay of N ms occupies slot {@code (currentTick + N) & 511} with
 * {@code rounds = N / 512}. Long delays (window of minutes, pipeline deadline) are
 * re-queued at most once per 512ms, keeping the timer thread idle between ticks.
 *
 * <h2>Thread safety</h2>
 * {@link #schedule} is safe to call from any thread, including virtual threads.
 * Task callbacks fire on the dedicated platform timer thread; they must be short
 * and non-blocking (typically {@link LockSupport#unpark} or {@link java.util.concurrent.atomic.AtomicBoolean#set}).
 */
public final class HeddleWheelTimer {

    public static final HeddleWheelTimer INSTANCE = new HeddleWheelTimer();

    private static final int  WHEEL_BITS = 9;
    private static final int  WHEEL_SIZE = 1 << WHEEL_BITS;   // 512
    private static final int  WHEEL_MASK = WHEEL_SIZE - 1;
    private static final long TICK_NS    = 1_000_000L;        // 1 ms in nanos

    private final AtomicReferenceArray<Node> wheel = new AtomicReferenceArray<>(WHEEL_SIZE);
    private volatile long currentTick = 0L;

    /** Cancellation token returned by {@link #schedule}. Safe to call from any thread. */
    public static final class TimerHandle {
        volatile boolean cancelled;
        public void cancel() { cancelled = true; }
    }

    private static final class Node {
        final long        rounds;
        final Runnable    task;
        final TimerHandle handle;
        final Node        next;

        Node(long rounds, Runnable task, TimerHandle handle, Node next) {
            this.rounds = rounds;
            this.task   = task;
            this.handle = handle;
            this.next   = next;
        }
    }

    private HeddleWheelTimer() {
        Thread.ofPlatform()
              .name("heddle-wheel-timer")
              .daemon(true)
              .start(this::spin);
    }

    /**
     * Schedules {@code task} to run after approximately {@code delayMs} milliseconds
     * on the timer platform thread. Returns a handle for cancellation; calling
     * {@link TimerHandle#cancel()} on an already-fired handle is a no-op.
     *
     * @param delayMs milliseconds to delay; values less than 1 are treated as 1
     * @param task    short-lived callback (must not block)
     * @return a cancellation handle
     */
    public TimerHandle schedule(long delayMs, Runnable task) {
        long ticks      = Math.max(1L, delayMs);
        long targetTick = currentTick + ticks;
        int  idx        = (int)(targetTick & WHEEL_MASK);
        long rounds     = ticks >> WHEEL_BITS;
        TimerHandle handle = new TimerHandle();
        push(idx, new Node(rounds, task, handle, null));
        return handle;
    }

    private void push(int idx, Node node) {
        Node head;
        do {
            head = wheel.get(idx);
        } while (!wheel.compareAndSet(idx, head, new Node(node.rounds, node.task, node.handle, head)));
    }

    private void spin() {
        long nextDeadlineNs = System.nanoTime();
        while (!Thread.currentThread().isInterrupted()) {
            nextDeadlineNs += TICK_NS;
            long parkNs = nextDeadlineNs - System.nanoTime();
            if (parkNs > 0) LockSupport.parkNanos(parkNs);

            int idx = (int)(++currentTick & WHEEL_MASK);
            Node head = wheel.getAndSet(idx, null);
            while (head != null) {
                Node n = head;
                head = n.next;
                if (n.handle.cancelled) continue;
                if (n.rounds > 0) {
                    push(idx, new Node(n.rounds - 1, n.task, n.handle, null));
                } else {
                    try { n.task.run(); } catch (Throwable ignored) {}
                }
            }
        }
    }
}
