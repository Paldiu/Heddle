package io.heddle.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Virtual thread factory and naming helpers.
 * All pipeline stage threads are virtual by default; set the system property
 * {@code heddle.platform.threads=true} to use platform threads instead
 * (useful for attaching debuggers or profilers that have limited VT support).
 */
public final class ThreadUtils {

    private ThreadUtils() {}

    private static final boolean PLATFORM_THREADS =
            Boolean.getBoolean("heddle.platform.threads");

    private static Thread.Builder threadBuilder() {
        return PLATFORM_THREADS ? Thread.ofPlatform() : Thread.ofVirtual();
    }

    /** Start a named thread and return it. */
    public static Thread startVirtual(String name, Runnable task) {
        return threadBuilder().name(name).start(task);
    }

    /**
     * Start all tasks as threads with auto-numbered names.
     * Names are formatted as {@code prefix-0}, {@code prefix-1}, …
     */
    public static Thread[] startAll(String prefix, Runnable... tasks) {
        Thread[] threads = new Thread[tasks.length];
        var builder = threadBuilder().name(prefix + "-", 0);
        for (int i = 0; i < tasks.length; i++) {
            threads[i] = builder.start(tasks[i]);
        }
        return threads;
    }

    /**
     * Create (but do not start) threads with auto-numbered names.
     * Callers can register the threads before starting them to avoid a
     * registration race.
     */
    public static Thread[] createAll(String prefix, Runnable... tasks) {
        Thread[] threads = new Thread[tasks.length];
        var builder = threadBuilder().name(prefix + "-", 0);
        for (int i = 0; i < tasks.length; i++) {
            threads[i] = builder.unstarted(tasks[i]);
        }
        return threads;
    }

    /**
     * Join all threads. If interrupted while waiting, the interrupt is
     * recorded and re-raised after all threads have been joined so that
     * callers see a clean join-all and the flag is never set while we're
     * about to call join() again (which would spin immediately).
     */
    public static void joinAll(Thread... threads) {
        boolean interrupted = false;
        for (Thread t : threads) {
            while (t.isAlive()) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    interrupted = true;
                    Thread.interrupted();
                }
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    /** Returns a counter-based name supplier for sequential stage IDs. */
    public static java.util.function.Supplier<String> nameSequence(String prefix) {
        AtomicLong counter = new AtomicLong();
        return () -> prefix + "-" + counter.getAndIncrement();
    }
}
