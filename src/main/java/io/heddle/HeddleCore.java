package io.heddle;

import io.heddle.api.NChannel;
import io.heddle.api.PipelineHandle;
import io.heddle.api.Sink;
import io.heddle.api.Stage;
import io.heddle.channel.HeddleChannel;
import io.heddle.config.PipelineOptions;
import io.heddle.context.PipelineContext;
import io.heddle.internal.PipelineHandleImpl;
import io.heddle.internal.ConcurrentFlatMapStage;
import io.heddle.internal.SourceDescriptor;
import io.heddle.internal.SourceStage;
import io.heddle.internal.PipelineStage;
import io.heddle.internal.SinkStage;
import io.heddle.internal.Transfer;
import io.heddle.internal.processor.AsyncTeeProcessor;
import io.heddle.internal.processor.BackpressurePolicyProcessor;
import io.heddle.internal.processor.BufferProcessor;
import io.heddle.internal.processor.FlatMapProcessor;
import io.heddle.internal.processor.LimitProcessor;

import io.heddle.error.ErrorStrategy;
import io.heddle.util.ThreadUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Internal orchestrator. Accepts a source descriptor, an ordered list of
 * {@link Stage} descriptors, an optional terminal {@link Sink}, and
 * {@link PipelineOptions}; allocates {@link HeddleChannel}s, wraps everything
 * in a structured-concurrency supervisor, and returns an un-started
 * {@link PipelineHandle}.
 *
 * <h2>Execution model</h2>
 * A single <em>supervisor</em> virtual thread manages all stage VTs. It binds
 * {@link PipelineContext#CURRENT} via {@link ScopedValue} before starting any
 * stage, giving two guarantees:
 * <ol>
 *   <li><b>Lifetime containment</b> — the supervisor joins all stage VTs before
 *       returning, so callers join only one thread regardless of pipeline width.</li>
 *   <li><b>Scoped-value inheritance</b> — all stage VTs are started inside the
 *       supervisor's binding scope and therefore inherit
 *       {@link PipelineContext#CURRENT}, enabling user code to call
 *       {@link PipelineContext#current()} without constructor injection.</li>
 * </ol>
 * <p>{@link java.util.concurrent.StructuredTaskScope} (preview in Java 25) would add a third guarantee:
 * automatic sibling cancellation on subtask failure. It is the intended long-term
 * replacement for the manual thread array in {@code buildSupervisor}; adopt it
 * once it graduates to final.</p>
 *
 * <h2>Channel layout</h2>
 * <pre>
 *   source → ch[0] → stage[0] → ch[1] → … → stage[N-1] → ch[N] → sink
 * </pre>
 * N + 1 channels for N stages.
 *
 * <p>Users never call this class directly — entry is through {@link Heddle}
 * (fluent) or {@link Heddle#wire}/{@link Heddle#run} (annotation-driven).
 */
@SuppressWarnings("preview")
public final class HeddleCore {

    private HeddleCore() {}

    public static PipelineHandle assembleFull(
            SourceDescriptor<?> sourceDesc,
            List<Stage<?, ?>> stages,
            Sink<?> sink,
            PipelineOptions options) {
        List<ErrorStrategy> allStop = Collections.nCopies(stages.size(), new ErrorStrategy.Stop());
        return assembleFull(sourceDesc, stages, allStop, sink, options);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static PipelineHandle assembleFull(
            SourceDescriptor<?> sourceDesc,
            List<Stage<?, ?>> stages,
            List<ErrorStrategy> strategies,
            Sink<?> sink,
            PipelineOptions options) {

        PipelineContext context = new PipelineContext(options);
        int bufSize   = options.bufferSize();
        String prefix = options.threadName();

        int channelCount = stages.size() + 1;
        List<HeddleChannel<?>> channels = new ArrayList<>(channelCount);
        for (int i = 0; i < channelCount; i++) {
            channels.add(new HeddleChannel<>(bufSize, context));
        }

        for (int i = 0; i < stages.size(); i++) {
            if (stages.get(i) instanceof BufferProcessor<?> bp) {
                channels.set(i + 1, new HeddleChannel<>(bp.capacity(), context));
            } else if (stages.get(i) instanceof BackpressurePolicyProcessor<?> bpp) {
                channels.set(i + 1, new HeddleChannel<>(bufSize, context, bpp.policy()));
            }
        }

        List<Runnable> runnables = new ArrayList<>(stages.size() + 2);

        NChannel rawHead = channels.get(0);
        Runnable sourceRunnable = ((SourceDescriptor) sourceDesc).build(prefix + "-source", rawHead, context);
        Runnable sourceStop = (sourceRunnable instanceof SourceStage<?> ss) ? ss::stop : () -> {};
        runnables.add(sourceRunnable);

        for (Stage<?, ?> stage : stages) {
            if (stage instanceof LimitProcessor<?> lp) {
                lp.setStopCallback(sourceStop);
            }
        }

        List<PipelineHandle> companions = new ArrayList<>();
        for (int i = 0; i < stages.size(); i++) {
            NChannel upstream   = channels.get(i);
            NChannel downstream = channels.get(i + 1);
            Stage<?, ?> stage   = stages.get(i);
            ErrorStrategy strategy = strategies.get(i);
            if (stage instanceof AsyncTeeProcessor<?> tee) {
                companions.add(tee.branchHandle());
            }
            if (stage instanceof FlatMapProcessor fmp && fmp.concurrency() > 1) {
                runnables.add(new ConcurrentFlatMapStage(
                        prefix + "-stage-" + i, upstream, downstream, fmp.fn(), fmp.concurrency(), context));
            } else {
                runnables.add(new PipelineStage<>(
                        prefix + "-stage-" + i, upstream, downstream, stage, context, null, strategy));
            }
        }

        NChannel rawLast = channels.get(stages.size());
        NChannel<Transfer<Object>> lastChannel = rawLast;
        Sink<Object> effectiveSink = sink != null ? (Sink<Object>) sink : item -> {};
        SinkStage<Object> sinkStage = new SinkStage<>(prefix + "-sink", lastChannel, effectiveSink, context);
        runnables.add(sinkStage);

        List<String> stageNames = new ArrayList<>(stages.size());
        for (int i = 0; i < stages.size(); i++) {
            stageNames.add(prefix + "-stage-" + i);
        }

        Runnable supervisorBody = buildSupervisor(runnables, context, prefix, sourceStop, options.deadline());
        return new PipelineHandleImpl(supervisorBody, context, prefix, sourceStop,
                Collections.unmodifiableList(channels),
                Collections.unmodifiableList(stageNames),
                sinkStage.itemsProcessed(),
                Collections.unmodifiableList(companions));
    }

    /**
     * Legacy overload: bare stage chain without source or sink.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static PipelineHandle assemble(List<Stage<?, ?>> stages, PipelineOptions options) {
        if (stages == null || stages.isEmpty()) {
            throw new IllegalArgumentException("pipeline must have at least one stage");
        }

        PipelineContext context = new PipelineContext();
        int bufSize   = options.bufferSize();
        String prefix = options.threadName();

        int channelCount = stages.size() + 1;
        List<HeddleChannel<?>> channels = new ArrayList<>(channelCount);
        for (int i = 0; i < channelCount; i++) {
            channels.add(new HeddleChannel<>(bufSize, context));
        }

        for (int i = 0; i < stages.size(); i++) {
            if (stages.get(i) instanceof BufferProcessor<?> bp) {
                channels.set(i + 1, new HeddleChannel<>(bp.capacity(), context));
            } else if (stages.get(i) instanceof BackpressurePolicyProcessor<?> bpp) {
                channels.set(i + 1, new HeddleChannel<>(bufSize, context, bpp.policy()));
            }
        }

        List<Runnable> runnables = new ArrayList<>(stages.size());
        for (int i = 0; i < stages.size(); i++) {
            NChannel upstream   = channels.get(i);
            NChannel downstream = channels.get(i + 1);
            Stage<?, ?> stage   = stages.get(i);
            if (stage instanceof FlatMapProcessor fmp && fmp.concurrency() > 1) {
                runnables.add(new ConcurrentFlatMapStage(
                        prefix + "-stage-" + i, upstream, downstream, fmp.fn(), fmp.concurrency(), context));
            } else {
                runnables.add(new PipelineStage<>(
                        prefix + "-stage-" + i, upstream, downstream, stage, context));
            }
        }

        Runnable supervisorBody = buildSupervisor(runnables, context, prefix, () -> {}, options.deadline());
        return new PipelineHandleImpl(supervisorBody, context, prefix, () -> {});
    }

    /**
     * Builds the supervisor task: binds {@link PipelineContext#CURRENT} and
     * {@link PipelineContext#SOURCE_STOP} as {@link ScopedValue}s on the supervisor
     * thread, then creates all stage VTs unstarted, pre-registers them with the
     * context (closing the {@link PipelineContext#interruptAll} race), starts them
     * all, and joins.
     *
     * <p>If {@code deadline} is non-null a watchdog virtual thread is started before
     * the stage threads. The watchdog sleeps the deadline duration and then calls
     * {@link PipelineContext#signalCancellation()}. It is interrupted immediately after
     * {@code joinAll} returns so it does not linger when the pipeline finishes normally.
     *
     * <p>The ScopedValue binding is inherited by all stage virtual threads because
     * they are started inside the binding's dynamic scope, giving user stage code
     * access to {@link PipelineContext#current()} anywhere in the call tree without
     * constructor injection.
     *
     * <p>Note: {@link java.util.concurrent.StructuredTaskScope} would be the
     * natural fit here (lifetime containment + failure cascade in one construct),
     * but it remains a preview API in Java 25. When it graduates to final the
     * body of this method becomes a {@code ShutdownOnFailure} scope with
     * {@code scope.fork()} replacing the manual thread array.
     */
    private static Runnable buildSupervisor(
            List<Runnable> runnables, PipelineContext context, String prefix,
            Runnable sourceStop, Duration deadline) {

        Runnable body = () -> {
            Thread watchdog = null;
            if (deadline != null) {
                watchdog = Thread.ofVirtual()
                        .name(prefix + "-watchdog")
                        .start(() -> {
                            try {
                                Thread.sleep(deadline);
                            } catch (InterruptedException ignored) {
                                return;
                            }
                            context.signalCancellation();
                        });
            }

            Thread[] stageThreads = ThreadUtils.createAll(prefix, runnables.toArray(new Runnable[0]));
            for (int i = 0; i < stageThreads.length; i++) {
                context.preRegister(prefix + "-" + i, stageThreads[i]);
            }
            for (Thread t : stageThreads) t.start();
            ThreadUtils.joinAll(stageThreads);
            context.signalCompletion();

            if (watchdog != null) watchdog.interrupt();
        };

        return () -> {
            ScopedValue.Carrier carrier = ScopedValue.where(PipelineContext.CURRENT, context)
                                                     .where(PipelineContext.SOURCE_STOP, sourceStop);
            if (deadline != null) {
                carrier = carrier.where(PipelineContext.DEADLINE, Instant.now().plus(deadline));
            }
            carrier.run(body);
        };
    }
}
