package io.heddle.internal.processor;

import io.heddle.api.Describable;
import io.heddle.api.Owned;
import io.heddle.api.Stage;
import io.heddle.api.StageContext;

import java.util.function.Consumer;

/**
 * Logs each item then passes it through unchanged. Uses a caller-supplied
 * {@link Consumer} printer so the library has no hard dependency on any
 * logging framework. The default printer writes to {@link System#Logger}
 * at INFO level (zero external dependencies).
 */
public final class LogProcessor<T> implements Stage<T, T>, Describable {

    private final Consumer<T> printer;

    /** Logs via JDK {@link System.Logger} at INFO, prefixed with {@code label}. */
    public LogProcessor(String label) {
        System.Logger logger = System.getLogger(LogProcessor.class.getName());
        String prefix = (label == null || label.isEmpty()) ? "" : "[" + label + "] ";
        this.printer = item -> logger.log(System.Logger.Level.INFO, prefix + item);
    }

    /** Logs using a custom printer. */
    public LogProcessor(Consumer<T> printer) {
        if (printer == null) throw new NullPointerException("printer must not be null");
        this.printer = printer;
    }

    @Override
    public void process(Owned<T> item, StageContext<T> ctx) {
        T value = item.consume();
        printer.accept(value);
        ctx.emit(value);
    }

    @Override
    public String describe() { return "LogProcessor"; }
}
