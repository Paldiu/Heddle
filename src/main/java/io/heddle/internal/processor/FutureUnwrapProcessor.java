package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.Stage;
import io.heddle.api.StageContext;
import io.heddle.error.HeddleException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Unwraps {@link CompletableFuture}{@code <T>} items into their resolved values.
 *
 * <p>This stage demonstrates the core Loom value proposition: the virtual thread
 * parks on {@link CompletableFuture#get()} without pinning a carrier thread, so
 * hundreds of in-flight futures can be awaited simultaneously at zero platform-thread
 * cost. No callbacks, no {@code thenApply} chains — just a blocking {@code get()}
 * that reads like synchronous code and performs like async.
 *
 * <p>If the future completed exceptionally the original cause is re-thrown, unwrapped
 * from the {@link ExecutionException} wrapper. A cancelled future is treated as a
 * pipeline failure.
 *
 * <p>If the current thread is interrupted while waiting (pipeline cancelled or failed),
 * the interrupt flag is restored and a {@link HeddleException} is thrown so the stage
 * loop exits cleanly.
 */
public final class FutureUnwrapProcessor<T> implements Stage<CompletableFuture<T>, T>, Describable {

    @Override
    public void process(Owned<CompletableFuture<T>> item, StageContext<T> ctx) {
        CompletableFuture<T> future = item.consume();
        try {
            
            ctx.emit(future.get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HeddleException("future unwrap interrupted — pipeline may be shutting down", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException re) throw re;
            throw new RuntimeException("future completed exceptionally", cause);
        }
    }

    @Override
    public String describe() { return "FutureUnwrapProcessor"; }
}
