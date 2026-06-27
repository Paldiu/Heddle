package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.StageContext;
import io.heddle.internal.transformer.StatefulTransformer;

import java.util.ArrayList;
import java.util.List;

/**
 * Collects up to {@code batchSize} items into a {@code List<T>} before emitting.
 * A partial batch is flushed on upstream completion so no items are lost.
 */
public final class BatchProcessor<T> extends StatefulTransformer<T, List<T>> implements Describable {

    private final int batchSize;
    private final List<T> buffer;

    private static final int MAX_BATCH_SIZE =
            Integer.parseInt(System.getProperty("heddle.max.batch.size", "1000000"));

    public BatchProcessor(int batchSize) {
        if (batchSize <= 0)
            throw new IllegalArgumentException("batchSize must be positive");
        if (batchSize > MAX_BATCH_SIZE)
            throw new IllegalArgumentException(
                    "batchSize " + batchSize + " exceeds maximum " + MAX_BATCH_SIZE +
                    " (override via system property heddle.max.batch.size)");
        this.batchSize = batchSize;
        this.buffer    = new ArrayList<>(batchSize);
    }

    @Override
    public void process(Owned<T> item, StageContext<List<T>> ctx) {
        buffer.add(item.consume());
        if (buffer.size() >= batchSize) {
            drain(ctx);
        }
    }

    @Override
    public void flush(StageContext<List<T>> ctx) {
        if (!buffer.isEmpty()) {
            drain(ctx);
        }
    }

    @Override
    public void reset() {
        buffer.clear();
    }

    private void drain(StageContext<List<T>> ctx) {
        ctx.emit(new ArrayList<>(buffer));
        buffer.clear();
    }

    @Override
    public String describe() { return "BatchProcessor(" + batchSize + ")"; }
}
