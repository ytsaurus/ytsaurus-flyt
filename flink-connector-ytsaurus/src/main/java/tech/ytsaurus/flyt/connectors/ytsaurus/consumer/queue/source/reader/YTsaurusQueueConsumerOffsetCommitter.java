package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;
import tech.ytsaurus.client.ApiServiceClient;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.AdvanceConsumer;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.core.cypress.YPath;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplit;

@Slf4j
public final class YTsaurusQueueConsumerOffsetCommitter implements YtQueueOffsetCommitter {
    private final ApiServiceClient client;

    private final YPath consumerPath;

    private final YPath queuePath;

    private final NavigableMap<Long, List<YtQueueSplit>> pendingCheckpoints = new TreeMap<>();

    private boolean closed;

    public YTsaurusQueueConsumerOffsetCommitter(
            ApiServiceClient client,
            String consumerPath,
            String queuePath) {
        this.client = Objects.requireNonNull(client, "client");
        this.consumerPath = YPath.simple(Objects.requireNonNull(consumerPath, "consumerPath"));
        this.queuePath = YPath.simple(Objects.requireNonNull(queuePath, "queuePath"));
        log.info(
                "Opened YT queue consumer offset committer for consumer {} and queue {}",
                this.consumerPath,
                this.queuePath);
    }

    @Override
    public synchronized void snapshotState(long checkpointId, List<YtQueueSplit> splits) {
        ensureOpen();
        Objects.requireNonNull(splits, "splits");
        List<YtQueueSplit> snapshot = new ArrayList<>(splits.size());
        Set<Integer> partitions = new HashSet<>();
        for (YtQueueSplit split : splits) {
            YtQueueSplit nonNullSplit = Objects.requireNonNull(split, "splits contains null");
            if (!partitions.add(nonNullSplit.getPartitionIndex())) {
                throw new IllegalArgumentException(
                        "Duplicate YT queue partition in checkpoint: " +
                                nonNullSplit.getPartitionIndex());
            }
            snapshot.add(nonNullSplit);
        }
        snapshot.sort(Comparator.comparingInt(YtQueueSplit::getPartitionIndex));
        pendingCheckpoints.put(checkpointId, List.copyOf(snapshot));
        log.debug(
                "Saved {} YT queue consumer offsets for checkpoint {}",
                snapshot.size(),
                checkpointId);
    }

    @Override
    public synchronized void notifyCheckpointComplete(long checkpointId) throws Exception {
        ensureOpen();
        List<YtQueueSplit> offsets = pendingCheckpoints.get(checkpointId);
        if (offsets == null) {
            log.debug("No pending YT queue consumer offsets for completed checkpoint {}", checkpointId);
            return;
        }

        log.info(
                "Committing {} YT queue consumer offsets for checkpoint {}",
                offsets.size(),
                checkpointId);
        try {
            if (!offsets.isEmpty()) {
                commitOffsets(offsets);
            }
        } catch (Exception | Error failure) {
            log.error(
                    "Failed to commit YT queue consumer offsets for checkpoint {}",
                    checkpointId,
                    failure);
            throw failure;
        }
        pendingCheckpoints.headMap(checkpointId, true).clear();
        log.info("Committed YT queue consumer offsets for checkpoint {}", checkpointId);
    }

    @Override
    public synchronized void notifyCheckpointAborted(long checkpointId) {
        ensureOpen();
        if (pendingCheckpoints.remove(checkpointId) != null) {
            log.debug("Discarded YT queue consumer offsets for aborted checkpoint {}", checkpointId);
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        pendingCheckpoints.clear();
        log.info("Closed YT queue consumer offset committer");
    }

    private void commitOffsets(List<YtQueueSplit> offsets) throws Exception {
        ApiServiceTransaction transaction = await(client.startTransaction(StartTransaction.tablet()));
        try {
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
                        "transaction returned null future"));
            }
            await(CompletableFuture.allOf(requests.toArray(new CompletableFuture<?>[0])));
            await(transaction.commit());
        } finally {
            transaction.close();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Queue consumer offset committer is closed");
        }
    }

    private static <T> T await(CompletableFuture<T> future) throws Exception {
        try {
            return Objects.requireNonNull(future, "future").get();
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
}
