package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import tech.ytsaurus.client.ApiServiceClient;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.AdvanceConsumer;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.core.cypress.YPath;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplit;

import static org.apache.flink.util.Preconditions.checkArgument;

@Slf4j
public final class YTsaurusQueueConsumerOffsetCommitter implements YtQueueOffsetCommitter {
    private final ApiServiceClient client;

    private final YPath consumerPath;

    private final YPath queuePath;

    private final Counter commitsSucceeded;

    private final Counter commitsFailed;

    private final long closeTimeoutMillis;

    private final NavigableMap<Long, List<YtQueueSplit>> pendingCheckpoints = new TreeMap<>();

    private final CompletableFuture<Void> failureFuture = new CompletableFuture<>();

    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(
            1, 1, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(1),
            new ExecutorThreadFactory("yt-consumer-offset-committer",
                    (thread, failure) -> failureFuture.completeExceptionally(failure)),
            new ThreadPoolExecutor.DiscardOldestPolicy());

    private final AtomicBoolean closed = new AtomicBoolean();

    public YTsaurusQueueConsumerOffsetCommitter(
            ApiServiceClient client,
            String consumerPath,
            String queuePath,
            Counter commitsSucceeded,
            Counter commitsFailed,
            long closeTimeoutMillis) {
        this.client = Objects.requireNonNull(client, "YT client must not be null");
        this.consumerPath = YPath.simple(Objects.requireNonNull(consumerPath, "Consumer path must not be null"));
        this.queuePath = YPath.simple(Objects.requireNonNull(queuePath, "Queue path must not be null"));
        this.commitsSucceeded = Objects.requireNonNull(commitsSucceeded, "Commit success counter must not be null");
        this.commitsFailed = Objects.requireNonNull(commitsFailed, "Commit failure counter must not be null");
        checkArgument(closeTimeoutMillis >= 0, "Consumer offset committer close timeout must not be negative");
        this.closeTimeoutMillis = closeTimeoutMillis;
        log.info(
                "Opened YT queue consumer offset committer for consumer {} and queue {}",
                this.consumerPath,
                this.queuePath);
    }

    @Override
    public synchronized void snapshotState(long checkpointId, List<YtQueueSplit> splits) {
        ensureOpen();
        Objects.requireNonNull(splits, "Checkpoint splits must not be null");
        List<YtQueueSplit> snapshot = new ArrayList<>(splits.size());
        Set<Integer> partitions = new HashSet<>();
        for (YtQueueSplit split : splits) {
            YtQueueSplit nonNullSplit = Objects.requireNonNull(
                    split, "Checkpoint splits must not contain null elements");
            if (!partitions.add(nonNullSplit.getPartitionIndex())) {
                throw new IllegalArgumentException(
                        "Duplicate YT queue partition in checkpoint: " +
                                nonNullSplit.getPartitionIndex());
            }
            snapshot.add(nonNullSplit);
        }
        snapshot.sort(Comparator.comparingInt(YtQueueSplit::getPartitionIndex));
        pendingCheckpoints.put(checkpointId, snapshot);
        log.debug(
                "Saved {} YT queue consumer offsets for checkpoint {}",
                snapshot.size(),
                checkpointId);
    }

    @Override
    public synchronized void notifyCheckpointComplete(long checkpointId) {
        if (closed.get() || failureFuture.isDone()) {
            return;
        }
        List<YtQueueSplit> offsets = pendingCheckpoints.get(checkpointId);
        if (offsets == null) {
            log.debug("No pending YT queue consumer offsets for completed checkpoint {}", checkpointId);
            return;
        }

        // Each snapshot contains all offsets owned by this reader; the latest completion subsumes earlier ones.
        pendingCheckpoints.headMap(checkpointId, true).clear();
        executor.execute(() -> commitCheckpoint(checkpointId, offsets));
    }

    @Override
    public synchronized void notifyCheckpointAborted(long checkpointId) {
        if (pendingCheckpoints.remove(checkpointId) != null) {
            log.debug("Discarded YT queue consumer offsets for aborted checkpoint {}", checkpointId);
        }
    }

    @Override
    public CompletableFuture<Void> getFailureFuture() {
        return failureFuture;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        synchronized (this) {
            pendingCheckpoints.clear();
            executor.getQueue().clear();
        }
        ExecutorUtils.gracefulShutdown(closeTimeoutMillis, TimeUnit.MILLISECONDS, executor);
        log.info("Closed YT queue consumer offset committer");
    }

    private void commitCheckpoint(long checkpointId, List<YtQueueSplit> offsets) {
        if (closed.get() || failureFuture.isDone() || offsets.isEmpty()) {
            return;
        }
        try {
            log.info("Committing {} YT queue consumer offsets for checkpoint {}", offsets.size(), checkpointId);
            commitOffsets(offsets);
            commitsSucceeded.inc();
            log.info("Committed YT queue consumer offsets for checkpoint {}", checkpointId);
        } catch (Exception failure) {
            // try-with-resources can suppress a close Error behind the original exception.
            for (Throwable suppressed : failure.getSuppressed()) {
                if (suppressed instanceof Error) {
                    failureFuture.completeExceptionally(suppressed);
                    return;
                }
            }
            if (failure instanceof InterruptedException) {
                if (!closed.get()) {
                    failureFuture.completeExceptionally(failure);
                }
            } else {
                commitsFailed.inc();
                log.warn("Failed to commit YT queue consumer offsets for checkpoint {}", checkpointId, failure);
            }
        } catch (Error failure) {
            failureFuture.completeExceptionally(failure);
        }
    }

    private void commitOffsets(List<YtQueueSplit> offsets) throws Exception {
        try (ApiServiceTransaction transaction = await(client.startTransaction(StartTransaction.tablet()))) {
            List<CompletableFuture<Void>> requests = new ArrayList<>(offsets.size());
            for (YtQueueSplit split : offsets) {
                AdvanceConsumer request = AdvanceConsumer.builder()
                        .setConsumerPath(consumerPath)
                        .setQueuePath(queuePath)
                        .setPartitionIndex(split.getPartitionIndex())
                        .setNewOffset(split.getNextOffset())
                        .build();
                requests.add(Objects.requireNonNull(
                        transaction.advanceConsumer(request),
                        "YT transaction returned a null future for advancing consumer offsets"));
            }
            await(CompletableFuture.allOf(requests.toArray(new CompletableFuture<?>[0])));
            await(transaction.commit());
        }
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        try {
            return Objects.requireNonNull(future, "YT transaction RPC returned a null future").get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Queue consumer offset committer is closed");
        }
    }
}
