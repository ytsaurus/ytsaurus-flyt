package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.utils.TypeConversions;
import tech.ytsaurus.client.TableWriter;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeBinarySerializer;
import tech.ytsaurus.ysontree.YTreeMapNode;

import tech.ytsaurus.flyt.connectors.datametrics.DataMetricsWriterDelegate;

/** All session transitions run in the Flink task mailbox; SDK futures complete independently. */
final class YtStaticSinkWriter<T> implements SinkWriter<T> {
    private final YtStaticSink<T> config;
    private final Supplier<StaticTableClient> clientFactory;
    private final ProcessingTimeService time;
    private final MailboxExecutor mailbox;
    private final DataMetricsWriterDelegate dataMetrics;
    private final TableSchema writeSchema;
    private final Counter publishedRows;
    private final Counter publishedBytes;
    private final Counter errors;
    private final Counter failedRows;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    private StaticTableClient client;
    private boolean tableReady;
    private volatile boolean closed;
    private boolean errorCounted;
    private boolean writingRecord;
    private boolean currentRecordAccepted;
    private boolean endOfInput;
    private volatile Session active;
    private volatile Session closing;
    private ScheduledFuture<?> rollTimer;
    private long generation;
    private volatile long lastCloseDurationMillis;
    private volatile long lastCommitTimestamp;
    private volatile long maxTracked;
    private volatile long lastTracked;

    YtStaticSinkWriter(YtStaticSink<T> config, Sink.InitContext context,
                      Supplier<StaticTableClient> clientFactory) {
        this.config = config;
        writeSchema = YtStaticTableClient.parseSchema(config.ysonSchema).toWrite();
        this.clientFactory = clientFactory;
        time = context.getProcessingTimeService();
        mailbox = context.getMailboxExecutor();
        MetricGroup group = context.metricGroup().addGroup(config.proxy).addGroup(config.path);
        publishedRows = group.counter("publishedRows");
        // Binary YSON bytes before wire framing/compression, not compressed chunk size.
        publishedBytes = group.counter("publishedYsonBytes");
        errors = group.counter("writeErrors");
        failedRows = group.counter("sumFailedRows");
        group.gauge("pendingRows", this::pendingRows);
        group.gauge("pendingCloses", () -> closing == null ? 0 : 1);
        group.gauge("lastCloseDurationMillis", () -> lastCloseDurationMillis);
        group.gauge("sumCommittedRows", publishedRows::getCount);
        group.gauge("lastCommitTimestamp", () -> lastCommitTimestamp);
        if (config.trackableField != null) {
            group.gauge("trackedField", () -> maxTracked);
            group.gauge("lastTrackedField", () -> lastTracked);
        }
        dataMetrics = DataMetricsWriterDelegate.create(config.dataMetricsConfig,
                config.rowType == null ? null : TypeConversions.fromLogicalToDataType(config.rowType));
        dataMetrics.open(context.metricGroup());
    }

    @Override
    public void write(T value, Context context) throws IOException, InterruptedException {
        checkHealthy();
        if (endOfInput) {
            throw new IOException("Cannot write after end of input");
        }
        writingRecord = true;
        currentRecordAccepted = false;
        try {
            collectCompletedClose();
            // Conversion and the SDK's serialization both happen before write returns.
            YTreeMapNode row = config.serializer.serialize(value);
            long bytes = ysonSize(row);
            Session session = active == null ? openSession() : active;
            long deadline = deadline();
            // The raw YTreeMapNode writer starts with an empty serializer name table.
            // Its write(rows) overload uses that empty schema, not the table metadata.
            while (!session.writer.write(Collections.singletonList(row), writeSchema)) {
                // false means NOT accepted. Retry exactly this detached row after readiness.
                await(session.writer.readyEvent(), deadline, "write readiness");
                checkHealthy();
            }
            session.rows++;
            currentRecordAccepted = true;
            session.bytes += bytes;
            if (config.rowType != null) {
                RowData input = (RowData) value;
                dataMetrics.extractValues(input).forEach(
                        (name, timestamp) -> session.timestamps.merge(name, timestamp, Long::max));
                if (config.trackableField != null && !input.isNullAt(config.trackableField.getIndex())) {
                    long tracked = config.trackableField.getConverter().convert(input, config.trackableField.getIndex());
                    session.maxTracked = Math.max(session.maxTracked, tracked);
                    session.lastTracked = tracked;
                    session.hasTracked = true;
                }
            }
            if (session.rows == 1) {
                scheduleRoll(session);
            }
        } catch (IOException | RuntimeException e) {
            throw fail(e);
        } finally {
            writingRecord = false;
        }
    }

    private Session openSession() throws IOException, InterruptedException {
        if (client == null) {
            client = clientFactory.get();
        }
        if (!tableReady) {
            await(client.ensureTable(), deadline(), "create/validate table");
            tableReady = true;
        }
        CompletableFuture<TableWriter<YTreeMapNode>> opening = client.openWriter();
        try {
            TableWriter<YTreeMapNode> writer = await(opening, deadline(), "open writer");
            checkHealthy();
            active = new Session(writer);
        } catch (IOException | InterruptedException | RuntimeException e) {
            // The future can complete between get throwing and failure/closed becoming visible.
            // Register unconditional cancellation, which also runs for an already completed open.
            opening.thenAccept(TableWriter::cancel);
            throw e;
        }
        return active;
    }

    private void scheduleRoll(Session session) {
        long currentGeneration = ++generation;
        rollTimer = time.registerTimer(time.getCurrentProcessingTime() + config.rollInterval.toMillis(), timestamp -> {
            if (!closed && active == session && generation == currentGeneration) {
                checkHealthy();
                roll();
            }
        });
    }

    private void roll() throws IOException, InterruptedException {
        checkHealthy();
        if (active == null) {
            return;
        }
        // At most one old upload may close while the next one accepts records.
        awaitClosing();
        invalidateTimer();
        Session session = active;
        active = null;
        closing = session;
        session.closeStarted = System.nanoTime();
        session.closeDeadline = deadline();
        try {
            // Never arrange completion through the mailbox: flush may be blocking that mailbox.
            session.completion = session.writer.close().thenApply(ignored -> (Void) null)
                    .orTimeout(config.writeTimeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (RuntimeException e) {
            throw fail(e);
        }
        session.completion.whenComplete((ignored, error) -> {
            if (error != null) {
                failure.compareAndSet(null, error);
            }
            if (!closed) {
                try {
                    mailbox.execute(() -> {
                        if (!closed) {
                            checkHealthy();
                            collectCompletedClose();
                        }
                    }, "Finish static YT upload");
                } catch (RuntimeException ignoredRejection) {
                    // Shutdown can reject queued work. The failure is still available to flush/write.
                }
            }
        });
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkHealthy();
        roll();
        awaitClosing();
        checkHealthy();
        this.endOfInput = endOfInput;
    }

    private void awaitClosing() throws IOException, InterruptedException {
        Session session = closing;
        if (session != null) {
            await(session.completion, session.closeDeadline, "close writer");
            publish(session);
        }
    }

    private void collectCompletedClose() throws IOException, InterruptedException {
        Session session = closing;
        if (session != null && session.completion.isDone()) {
            awaitClosing();
        }
    }

    private void publish(Session session) {
        if (closed || closing != session) {
            return;
        }
        publishedRows.inc(session.rows);
        publishedBytes.inc(session.bytes);
        lastCloseDurationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - session.closeStarted);
        lastCommitTimestamp = System.currentTimeMillis();
        if (session.hasTracked) {
            maxTracked = Math.max(maxTracked, session.maxTracked);
            lastTracked = session.lastTracked;
        }
        dataMetrics.onCommit(session.timestamps);
        closing = null;
    }

    private long pendingRows() {
        Session current = active;
        Session previous = closing;
        return (current == null ? 0 : current.rows) + (previous == null ? 0 : previous.rows);
    }

    private long deadline() {
        return System.nanoTime() + config.writeTimeout.toNanos();
    }

    private <V> V await(CompletableFuture<V> future, long deadline, String operation)
            throws IOException, InterruptedException {
        try {
            return future.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        } catch (ExecutionException | TimeoutException | RuntimeException e) {
            throw fail(new IOException("Static YT " + operation + " failed", e));
        }
    }

    private void checkHealthy() throws IOException {
        if (closed) {
            throw new IOException("Static YT writer is closed");
        }
        if (failure.get() != null) {
            throw fail(failure.get());
        }
    }

    private IOException fail(Throwable error) {
        failure.compareAndSet(null, error);
        if (!errorCounted) {
            errors.inc();
            failedRows.inc(pendingRows() + (writingRecord && !currentRecordAccepted ? 1 : 0));
            errorCounted = true;
        }
        return new IOException("Static YT upload failed; checkpoint must not be acknowledged", error);
    }

    private void invalidateTimer() {
        ++generation;
        if (rollTimer != null) {
            rollTimer.cancel(false);
            rollTimer = null;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        invalidateTimer();
        try {
            if (active != null) {
                active.writer.cancel();
            }
        } finally {
            try {
                if (closing != null) {
                    closing.writer.cancel();
                }
            } finally {
                active = null;
                closing = null;
                dataMetrics.close();
                if (client != null) {
                    client.close();
                }
            }
        }
    }

    private static long ysonSize(YTreeMapNode row) {
        CountingOutputStream output = new CountingOutputStream();
        YTreeBinarySerializer.serialize(row, output);
        return output.count;
    }

    private static final class CountingOutputStream extends OutputStream {
        private long count;
        @Override
        public void write(int value) { count++; }
        @Override
        public void write(byte[] value, int offset, int length) { count += length; }
    }

    private static final class Session {
        private final TableWriter<YTreeMapNode> writer;
        private final Map<String, Long> timestamps = new HashMap<>();
        private volatile long rows;
        private long bytes;
        private long maxTracked = Long.MIN_VALUE;
        private long lastTracked;
        private boolean hasTracked;
        private long closeStarted;
        private long closeDeadline;
        private CompletableFuture<Void> completion;

        private Session(TableWriter<YTreeMapNode> writer) { this.writer = writer; }
    }
}
