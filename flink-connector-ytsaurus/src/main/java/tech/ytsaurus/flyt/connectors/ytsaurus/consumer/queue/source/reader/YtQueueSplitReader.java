package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueTrimmedOffsetPolicy;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueReaderOptions;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueueBatch;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueuePullRequest;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueueRawRecord;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplit;

public final class YtQueueSplitReader implements SplitReader<YtQueueRawRecord, YtQueueSplit> {
    private static final int WORKER_CLOSE_TIMEOUT_SECONDS = 10;

    private static final AtomicInteger WORKER_THREAD_SEQUENCE = new AtomicInteger();

    private static final BufferedResult WAKE_UP_SIGNAL = BufferedResult.wakeUpSignal();

    private final Supplier<? extends YtQueuePuller> pullerSupplier;

    private final int maxRows;

    private final long maxDataWeightBytes;

    private final long emptyPollBackoffNanos;

    private final YtQueueTrimmedOffsetPolicy trimmedOffsetPolicy;

    private final int configuredWorkerCount;

    private final LinkedBlockingQueue<BufferedResult> readBuffer;

    private final ExecutorService workerExecutor;

    private final Map<String, SplitCursor> splits = new LinkedHashMap<>();

    private final Set<String> pausedSplits = new HashSet<>();

    private final Set<String> finishedSplits = new HashSet<>();

    private final List<Worker> workers = new ArrayList<>();

    private final Object stateMonitor = new Object();

    private final AtomicBoolean wakeUpRequested = new AtomicBoolean();

    private final AtomicBoolean wakeUpSignalQueued = new AtomicBoolean();

    private final AtomicBoolean closed = new AtomicBoolean();

    private final AtomicReference<Throwable> workerFailure = new AtomicReference<>();

    private final ConcurrentLinkedQueue<Throwable> closeFailures = new ConcurrentLinkedQueue<>();

    private int nextSplitIndex;

    public YtQueueSplitReader(
            Supplier<? extends YtQueuePuller> pullerSupplier,
            YtQueueReaderOptions options) {
        this(pullerSupplier, options, YtQueueTrimmedOffsetPolicy.FAIL);
    }

    public YtQueueSplitReader(
            Supplier<? extends YtQueuePuller> pullerSupplier,
            YtQueueReaderOptions options,
            YtQueueTrimmedOffsetPolicy trimmedOffsetPolicy) {
        this.pullerSupplier = Objects.requireNonNull(pullerSupplier, "pullerSupplier");
        Objects.requireNonNull(options, "options");
        this.maxRows = options.getMaxRows();
        this.maxDataWeightBytes = options.getMaxDataWeightBytes();
        this.emptyPollBackoffNanos = options.getEmptyPollBackoff().toNanos();
        this.trimmedOffsetPolicy = trimmedOffsetPolicy;
        this.configuredWorkerCount = options.getWorkerCount();
        this.readBuffer = new LinkedBlockingQueue<>(options.getBufferCapacity());
        this.workerExecutor = Executors.newFixedThreadPool(
                configuredWorkerCount,
                YtQueueSplitReader::createWorkerThread);
    }

    @Override
    public RecordsWithSplitIds<YtQueueRawRecord> fetch() throws IOException {
        while (!closed.get()) {
            throwIfWorkerFailed();
            if (consumeWakeUpRequest()) {
                return emptyResult();
            }

            RecordsWithSplitIds<YtQueueRawRecord> finishedResult = takeFinishedSplits();
            if (finishedResult != null) {
                return finishedResult;
            }

            BufferedResult bufferedResult;
            try {
                bufferedResult = readBuffer.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for queue data", e);
            }

            if (bufferedResult == WAKE_UP_SIGNAL) {
                wakeUpSignalQueued.set(false);
                if (wakeUpRequested.compareAndSet(true, false)) {
                    return emptyResult();
                }
                continue;
            }

            synchronized (stateMonitor) {
                SplitCursor cursor = bufferedResult.cursor;
                if (cursor == null || splits.get(cursor.split.splitId()) != cursor) {
                    continue;
                }
            }
            return Objects.requireNonNull(bufferedResult.records);
        }
        return emptyResult();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<YtQueueSplit> splitsChange) {
        Objects.requireNonNull(splitsChange, "splitsChange");
        if (splitsChange instanceof SplitsAddition) {
            synchronized (stateMonitor) {
                for (YtQueueSplit split : splitsChange.splits()) {
                    Objects.requireNonNull(split, "splitsChange contains null");
                    if (splits.putIfAbsent(split.splitId(), new SplitCursor(split)) != null) {
                        throw new IllegalArgumentException("Split is already assigned: " + split.splitId());
                    }
                    pausedSplits.remove(split.splitId());
                    finishedSplits.remove(split.splitId());
                }
                normalizeNextSplitIndex();
                stateMonitor.notifyAll();
            }
            reconcileWorkers();
            return;
        }
        if (splitsChange instanceof SplitsRemoval) {
            Set<String> removedSplitIds = new HashSet<>();
            List<Worker> workersToWake;
            synchronized (stateMonitor) {
                for (YtQueueSplit split : splitsChange.splits()) {
                    Objects.requireNonNull(split, "splitsChange contains null");
                    if (splits.remove(split.splitId()) != null) {
                        finishedSplits.add(split.splitId());
                        removedSplitIds.add(split.splitId());
                    }
                    pausedSplits.remove(split.splitId());
                }
                normalizeNextSplitIndex();
                workersToWake = new ArrayList<>(workers);
                stateMonitor.notifyAll();
            }
            if (!removedSplitIds.isEmpty()) {
                readBuffer.removeIf(result -> result != WAKE_UP_SIGNAL
                        && result.cursor != null
                        && removedSplitIds.contains(result.cursor.split.splitId()));
                enqueueWakeUpSignal();
                workersToWake.forEach(worker -> worker.wakeUpIfReading(removedSplitIds));
            }
            reconcileWorkers();
            return;
        }
        throw new IllegalArgumentException("Unsupported split change: " + splitsChange.getClass().getName());
    }

    @Override
    public void pauseOrResumeSplits(
            Collection<YtQueueSplit> splitsToPause,
            Collection<YtQueueSplit> splitsToResume) {
        synchronized (stateMonitor) {
            for (YtQueueSplit split : splitsToPause) {
                if (splits.containsKey(split.splitId())) {
                    pausedSplits.add(split.splitId());
                }
            }
            for (YtQueueSplit split : splitsToResume) {
                pausedSplits.remove(split.splitId());
            }
            stateMonitor.notifyAll();
        }
    }

    @Override
    public void wakeUp() {
        wakeUpRequested.set(true);
        enqueueWakeUpSignal();
    }

    @Override
    public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        List<Worker> workersToStop;
        synchronized (stateMonitor) {
            workersToStop = new ArrayList<>(workers);
            workers.clear();
            stateMonitor.notifyAll();
        }
        workersToStop.forEach(Worker::requestStop);
        wakeUpRequested.set(true);
        enqueueWakeUpSignal();

        workerExecutor.shutdownNow();
        boolean terminated;
        try {
            terminated = workerExecutor.awaitTermination(WORKER_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while closing queue reader workers", e);
        }
        if (!terminated) {
            throw new IOException("Timed out while closing queue reader workers");
        }

        Throwable closeFailure = closeFailures.poll();
        if (closeFailure != null) {
            IOException failure = new IOException("Failed to close queue reader worker", closeFailure);
            closeFailures.forEach(failure::addSuppressed);
            throw failure;
        }
    }

    @VisibleForTesting
    int getWorkerCount() {
        synchronized (stateMonitor) {
            return workers.size();
        }
    }

    @VisibleForTesting
    int getBufferedBatchCount() {
        return (int) readBuffer.stream().filter(result -> result != WAKE_UP_SIGNAL).count();
    }

    private void reconcileWorkers() {
        List<Worker> workersToStart = new ArrayList<>();
        List<Worker> workersToStop = new ArrayList<>();
        synchronized (stateMonitor) {
            if (closed.get()) {
                return;
            }
            int targetWorkerCount = Math.min(configuredWorkerCount, splits.size());
            while (workers.size() > targetWorkerCount) {
                workersToStop.add(workers.remove(workers.size() - 1));
            }
            while (workers.size() < targetWorkerCount) {
                Worker worker = new Worker();
                workers.add(worker);
                workersToStart.add(worker);
            }
            stateMonitor.notifyAll();
        }

        workersToStop.forEach(Worker::requestStop);
        for (Worker worker : workersToStart) {
            try {
                worker.setTask(workerExecutor.submit(worker));
            } catch (RejectedExecutionException e) {
                if (!closed.get()) {
                    registerWorkerFailure(e);
                }
            }
        }
    }

    @Nullable
    private SplitCursor awaitEligibleSplit(Worker worker) throws InterruptedException {
        synchronized (stateMonitor) {
            while (!closed.get() && !worker.stopRequested.get() && workerFailure.get() == null) {
                long now = System.nanoTime();
                List<SplitCursor> currentSplits = new ArrayList<>(splits.values());
                SplitCursor selected = selectEligibleSplit(currentSplits, now);
                if (selected != null) {
                    selected.inFlight = true;
                    return selected;
                }

                long waitNanos = waitNanos(currentSplits, now);
                if (waitNanos == Long.MAX_VALUE) {
                    stateMonitor.wait();
                } else {
                    TimeUnit.NANOSECONDS.timedWait(stateMonitor, waitNanos);
                }
            }
            return null;
        }
    }

    @Nullable
    private SplitCursor selectEligibleSplit(List<SplitCursor> currentSplits, long now) {
        if (currentSplits.isEmpty()) {
            return null;
        }
        normalizeNextSplitIndex(currentSplits.size());
        for (int offset = 0; offset < currentSplits.size(); offset++) {
            int index = (nextSplitIndex + offset) % currentSplits.size();
            SplitCursor cursor = currentSplits.get(index);
            if (!pausedSplits.contains(cursor.split.splitId())
                    && !cursor.inFlight
                    && cursor.nextPollNanos - now <= 0) {
                nextSplitIndex = (index + 1) % currentSplits.size();
                return cursor;
            }
        }
        return null;
    }

    private long waitNanos(List<SplitCursor> currentSplits, long now) {
        long waitNanos = Long.MAX_VALUE;
        for (SplitCursor cursor : currentSplits) {
            if (!pausedSplits.contains(cursor.split.splitId())
                    && !cursor.inFlight) {
                waitNanos = Math.min(waitNanos, Math.max(1, cursor.nextPollNanos - now));
            }
        }
        return waitNanos;
    }

    private YtQueuePullRequest createPullRequest(SplitCursor cursor) {
        synchronized (stateMonitor) {
            return new YtQueuePullRequest(
                    cursor.split.getPartitionIndex(),
                    cursor.nextFetchOffset,
                    maxRows,
                    maxDataWeightBytes);
        }
    }

    private void handleBatch(
            SplitCursor cursor,
            YtQueuePullRequest request,
            YtQueueBatch batch) throws IOException, InterruptedException {
        Objects.requireNonNull(batch, "queue puller returned null batch");
        if (batch.getStartOffset() != request.getOffset()) {
            if (batch.getStartOffset() < request.getOffset()
                    || trimmedOffsetPolicy == YtQueueTrimmedOffsetPolicy.FAIL) {
                throw new IOException(String.format(
                        "Queue partition %d returned start offset %d for requested offset %d",
                        request.getPartitionIndex(),
                        batch.getStartOffset(),
                        request.getOffset()));
            }
        }

        if (batch.getRows().isEmpty()) {
            synchronized (stateMonitor) {
                if (splits.get(cursor.split.splitId()) == cursor) {
                    cursor.nextFetchOffset = batch.getFinishOffset();
                    cursor.nextPollNanos = System.nanoTime() + emptyPollBackoffNanos;
                    cursor.inFlight = false;
                    stateMonitor.notifyAll();
                }
            }
            return;
        }

        BufferedResult bufferedResult = BufferedResult.records(cursor, records(cursor, batch));
        synchronized (stateMonitor) {
            if (splits.get(cursor.split.splitId()) != cursor) {
                cursor.inFlight = false;
                stateMonitor.notifyAll();
                return;
            }
            cursor.nextFetchOffset = batch.getFinishOffset();
        }

        try {
            readBuffer.put(bufferedResult);
        } catch (InterruptedException e) {
            synchronized (stateMonitor) {
                if (splits.get(cursor.split.splitId()) == cursor
                        && cursor.inFlight) {
                    cursor.nextFetchOffset = request.getOffset();
                    cursor.inFlight = false;
                    stateMonitor.notifyAll();
                }
            }
            throw e;
        }

        synchronized (stateMonitor) {
            cursor.inFlight = false;
            stateMonitor.notifyAll();
        }
    }

    private RecordsWithSplitIds<YtQueueRawRecord> records(
            SplitCursor cursor,
            YtQueueBatch batch) {
        RecordsBySplits.Builder<YtQueueRawRecord> records = new RecordsBySplits.Builder<>();
        for (int index = 0; index < batch.getRows().size(); index++) {
            records.add(cursor.split.splitId(), new YtQueueRawRecord(
                    cursor.split.getPartitionIndex(),
                    batch.getStartOffset() + index,
                    batch.getRows().get(index),
                    batch.getSchema()));
        }
        return records.build();
    }

    private void releaseInFlight(SplitCursor cursor) {
        synchronized (stateMonitor) {
            if (splits.get(cursor.split.splitId()) == cursor) {
                cursor.inFlight = false;
                stateMonitor.notifyAll();
            }
        }
    }

    private boolean isActive(SplitCursor cursor) {
        synchronized (stateMonitor) {
            return splits.get(cursor.split.splitId()) == cursor;
        }
    }

    private boolean consumeWakeUpRequest() {
        if (!wakeUpRequested.compareAndSet(true, false)) {
            return false;
        }
        if (readBuffer.remove(WAKE_UP_SIGNAL)) {
            wakeUpSignalQueued.set(false);
        }
        return true;
    }

    private void enqueueWakeUpSignal() {
        if (wakeUpSignalQueued.compareAndSet(false, true)
                && !readBuffer.offer(WAKE_UP_SIGNAL)) {
            wakeUpSignalQueued.set(false);
        }
    }

    @Nullable
    private RecordsWithSplitIds<YtQueueRawRecord> takeFinishedSplits() {
        synchronized (stateMonitor) {
            if (finishedSplits.isEmpty()) {
                return null;
            }
            RecordsBySplits.Builder<YtQueueRawRecord> result = new RecordsBySplits.Builder<>();
            result.addFinishedSplits(finishedSplits);
            finishedSplits.clear();
            return result.build();
        }
    }

    private void throwIfWorkerFailed() throws IOException {
        Throwable failure = workerFailure.get();
        if (failure == null) {
            return;
        }
        if (failure instanceof IOException) {
            throw (IOException) failure;
        }
        throw new IOException("Queue reader worker failed", failure);
    }

    private void registerWorkerFailure(Throwable failure) {
        Throwable unwrapped = unwrapCompletionException(failure);
        if (workerFailure.compareAndSet(null, unwrapped)) {
            enqueueWakeUpSignal();
            synchronized (stateMonitor) {
                stateMonitor.notifyAll();
            }
        }
    }

    private static YtQueueBatch await(CompletableFuture<YtQueueBatch> future) throws Exception {
        try {
            return Objects.requireNonNull(future, "queue puller returned null future").get();
        } catch (ExecutionException e) {
            Throwable cause = unwrapCompletionException(e.getCause());
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private static Throwable unwrapCompletionException(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static RecordsWithSplitIds<YtQueueRawRecord> emptyResult() {
        return new RecordsBySplits.Builder<YtQueueRawRecord>().build();
    }

    private void normalizeNextSplitIndex() {
        normalizeNextSplitIndex(splits.size());
    }

    private void normalizeNextSplitIndex(int size) {
        nextSplitIndex = size == 0 ? 0 : nextSplitIndex % size;
    }

    private static Thread createWorkerThread(Runnable task) {
        Thread thread = new Thread(
                task,
                "yt-queue-reader-worker-" + WORKER_THREAD_SEQUENCE.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    }

    private final class Worker implements Runnable {
        private final AtomicBoolean stopRequested = new AtomicBoolean();

        @Nullable
        private volatile YtQueuePuller puller;

        @Nullable
        private volatile Future<?> task;

        @Nullable
        private volatile SplitCursor currentCursor;

        @Override
        public void run() {
            YtQueuePuller workerPuller = null;
            try {
                if (stopRequested.get() || closed.get()) {
                    return;
                }
                workerPuller = Objects.requireNonNull(
                        pullerSupplier.get(),
                        "pullerSupplier returned null");
                puller = workerPuller;
                if (stopRequested.get() || closed.get()) {
                    return;
                }

                while (!stopRequested.get() && !closed.get() && workerFailure.get() == null) {
                    SplitCursor cursor = awaitEligibleSplit(this);
                    if (cursor == null) {
                        return;
                    }
                    currentCursor = cursor;
                    try {
                        if (!isActive(cursor)) {
                            releaseInFlight(cursor);
                            continue;
                        }
                        YtQueuePullRequest request = createPullRequest(cursor);
                        CompletableFuture<YtQueueBatch> pullFuture = workerPuller.pull(request);
                        if (!isActive(cursor)) {
                            workerPuller.wakeUp();
                        }
                        YtQueueBatch batch = await(pullFuture);
                        if (stopRequested.get() || closed.get()) {
                            releaseInFlight(cursor);
                            return;
                        }
                        handleBatch(cursor, request, batch);
                    } catch (Throwable failure) {
                        releaseInFlight(cursor);
                        if (stopRequested.get() || closed.get()) {
                            return;
                        }
                        if (!isActive(cursor)) {
                            continue;
                        }
                        if (failure instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        registerWorkerFailure(failure);
                        return;
                    } finally {
                        currentCursor = null;
                    }
                }
            } catch (Throwable failure) {
                if (!stopRequested.get() && !closed.get()) {
                    registerWorkerFailure(failure);
                }
            } finally {
                puller = null;
                if (workerPuller != null) {
                    try {
                        workerPuller.close();
                    } catch (Throwable failure) {
                        if (closed.get() || stopRequested.get()) {
                            closeFailures.add(failure);
                        } else {
                            registerWorkerFailure(failure);
                        }
                    }
                }
            }
        }

        private void setTask(Future<?> task) {
            this.task = task;
            if (stopRequested.get()) {
                task.cancel(true);
            }
        }

        private void requestStop() {
            stopRequested.set(true);
            YtQueuePuller currentPuller = puller;
            if (currentPuller != null) {
                currentPuller.wakeUp();
            }
            Future<?> currentTask = task;
            if (currentTask != null) {
                currentTask.cancel(true);
            }
            synchronized (stateMonitor) {
                stateMonitor.notifyAll();
            }
        }

        private void wakeUpIfReading(Set<String> splitIds) {
            SplitCursor cursor = currentCursor;
            YtQueuePuller currentPuller = puller;
            if (cursor != null
                    && currentPuller != null
                    && splitIds.contains(cursor.split.splitId())) {
                currentPuller.wakeUp();
            }
        }
    }

    private static final class BufferedResult {
        @Nullable
        private final SplitCursor cursor;

        @Nullable
        private final RecordsWithSplitIds<YtQueueRawRecord> records;

        private BufferedResult(
                @Nullable SplitCursor cursor,
                @Nullable RecordsWithSplitIds<YtQueueRawRecord> records) {
            this.cursor = cursor;
            this.records = records;
        }

        private static BufferedResult wakeUpSignal() {
            return new BufferedResult(null, null);
        }

        private static BufferedResult records(
                SplitCursor cursor,
                RecordsWithSplitIds<YtQueueRawRecord> records) {
            return new BufferedResult(
                    Objects.requireNonNull(cursor),
                    Objects.requireNonNull(records));
        }
    }

    private static final class SplitCursor {
        private final YtQueueSplit split;

        private long nextFetchOffset;

        private long nextPollNanos;

        private boolean inFlight;

        private SplitCursor(YtQueueSplit split) {
            this.split = split;
            this.nextFetchOffset = split.getNextOffset();
        }
    }
}
