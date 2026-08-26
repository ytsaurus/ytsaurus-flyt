package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.initializer.YtQueueOffsetInitializer;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.metadata.YtQueueMetadata;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.metadata.YtQueueMetadataProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplit;

@Slf4j
public final class YtQueueSplitEnumerator
        implements SplitEnumerator<YtQueueSplit, YtQueueEnumeratorState> {
    private final SplitEnumeratorContext<YtQueueSplit> context;

    private final YtQueueMetadataProvider metadataProvider;

    private final YtQueueOffsetInitializer offsetInitializer;

    private final long discoveryIntervalMillis;

    private final NavigableMap<Integer, YtQueueSplit> unassignedSplits = new TreeMap<>();

    private final AtomicLong discoverySequence = new AtomicLong();

    @Nullable
    private String queueObjectId;

    private int initializedPartitionCount;

    private long lastHandledDiscoverySequence = -1;

    private boolean metadataValidated;

    private boolean initializationInProgress;

    private boolean started;

    private boolean closed;

    @Nullable
    private YtQueueMetadata latestMetadata;

    public YtQueueSplitEnumerator(
            SplitEnumeratorContext<YtQueueSplit> context,
            YtQueueMetadataProvider metadataProvider,
            YtQueueOffsetInitializer offsetInitializer,
            long discoveryIntervalMillis) {
        this(
                context,
                metadataProvider,
                offsetInitializer,
                discoveryIntervalMillis,
                YtQueueEnumeratorState.empty());
    }

    public YtQueueSplitEnumerator(
            SplitEnumeratorContext<YtQueueSplit> context,
            YtQueueMetadataProvider metadataProvider,
            YtQueueOffsetInitializer offsetInitializer,
            long discoveryIntervalMillis,
            YtQueueEnumeratorState restoredState) {
        this.context = Objects.requireNonNull(context, "context");
        this.metadataProvider = Objects.requireNonNull(metadataProvider, "metadataProvider");
        this.offsetInitializer = Objects.requireNonNull(offsetInitializer, "offsetInitializer");
        if (discoveryIntervalMillis <= 0) {
            throw new IllegalArgumentException("discoveryIntervalMillis must be positive");
        }
        this.discoveryIntervalMillis = discoveryIntervalMillis;

        YtQueueEnumeratorState state = Objects.requireNonNull(restoredState, "restoredState");
        this.queueObjectId = state.getQueueObjectId();
        this.initializedPartitionCount = state.getInitializedPartitionCount();
        for (YtQueueSplit split : state.getUnassignedSplits()) {
            unassignedSplits.put(split.getPartitionIndex(), split);
        }
    }

    @Override
    public void start() {
        if (started || closed) {
            return;
        }
        started = true;
        log.info(
                "Starting YT queue split enumerator with {} initialized partitions and {} unassigned splits",
                initializedPartitionCount,
                unassignedSplits.size());
        context.callAsync(
                this::discoverMetadata,
                this::handleDiscovery,
                0,
                discoveryIntervalMillis);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        assignAvailableSplits();
    }

    @Override
    public void addSplitsBack(List<YtQueueSplit> splits, int subtaskId) {
        Objects.requireNonNull(splits, "splits");
        for (YtQueueSplit split : splits) {
            validateReturnedSplit(Objects.requireNonNull(split, "splits contains null"));
        }
        for (YtQueueSplit split : splits) {
            unassignedSplits.merge(
                    split.getPartitionIndex(),
                    split,
                    (current, returned) -> returned.getNextOffset() < current.getNextOffset()
                            ? returned
                            : current);
        }
        log.info("Returned YT queue splits from subtask {}: {}", subtaskId, splits);
        assignAvailableSplits();
    }

    @Override
    public void addReader(int subtaskId) {
        assignAvailableSplits();
    }

    @Override
    public YtQueueEnumeratorState snapshotState(long checkpointId) {
        YtQueueEnumeratorState state = new YtQueueEnumeratorState(
                queueObjectId,
                initializedPartitionCount,
                new ArrayList<>(unassignedSplits.values()));
        log.info(
                "Snapshotted YT queue split enumerator state for checkpoint {}: "
                        + "{} initialized partitions, {} unassigned splits",
                checkpointId,
                initializedPartitionCount,
                unassignedSplits.size());
        return state;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        log.info("Closing YT queue split enumerator");
        Exception failure = null;
        try {
            metadataProvider.close();
        } catch (Exception e) {
            failure = e;
        }
        try {
            offsetInitializer.close();
        } catch (Exception e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            log.error("Failed to close YT queue split enumerator", failure);
            if (failure instanceof IOException) {
                throw (IOException) failure;
            }
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
            throw new IOException("Failed to close YT queue enumerator", failure);
        }
        log.info("Closed YT queue split enumerator");
    }

    private DiscoveryAttempt discoverMetadata() {
        long sequence = discoverySequence.incrementAndGet();
        try {
            return DiscoveryAttempt.success(sequence, metadataProvider.getMetadata());
        } catch (Exception e) {
            return DiscoveryAttempt.failure(sequence, e);
        }
    }

    private void handleDiscovery(DiscoveryAttempt attempt, Throwable asyncError) {
        if (closed) {
            return;
        }
        if (asyncError != null) {
            throw new RuntimeException("Failed to discover YT queue metadata", asyncError);
        }
        if (attempt == null) {
            throw new IllegalStateException("YT queue metadata discovery returned no result");
        }
        if (attempt.sequence <= lastHandledDiscoverySequence) {
            return;
        }
        lastHandledDiscoverySequence = attempt.sequence;
        if (attempt.error != null) {
            throw new RuntimeException("Failed to discover YT queue metadata", attempt.error);
        }
        handleMetadata(Objects.requireNonNull(attempt.metadata));
    }

    private void handleMetadata(YtQueueMetadata metadata) {
        YtQueueMetadata previousMetadata = latestMetadata;
        if (queueObjectId == null) {
            queueObjectId = metadata.getQueueObjectId();
        } else if (!queueObjectId.equals(metadata.getQueueObjectId())) {
            throw new IllegalStateException(
                    "YT queue was recreated: expected object id " + queueObjectId +
                            ", discovered " + metadata.getQueueObjectId());
        }
        latestMetadata = metadata;
        metadataValidated = true;
        if (previousMetadata == null
                || previousMetadata.getPartitionCount() != metadata.getPartitionCount()) {
            log.info(
                    "Discovered YT queue {} with {} partitions",
                    metadata.getQueueObjectId(),
                    metadata.getPartitionCount());
        }
        assignAvailableSplits();
        if (!initializationInProgress
                && initializedPartitionCount < metadata.getPartitionCount()) {
            startOffsetInitialization(metadata);
        }
    }

    private void startOffsetInitialization(YtQueueMetadata metadata) {
        int firstPartition = initializedPartitionCount;
        int partitionCount = metadata.getPartitionCount();
        initializationInProgress = true;
        log.info(
                "Initializing YT queue offsets for partitions {}..{}",
                firstPartition,
                partitionCount - 1);
        context.callAsync(
                () -> initializeSplits(metadata, firstPartition, partitionCount),
                (splits, error) -> handleInitializedSplits(
                        metadata,
                        firstPartition,
                        partitionCount,
                        splits,
                        error));
    }

    private List<YtQueueSplit> initializeSplits(
            YtQueueMetadata metadata,
            int firstPartition,
            int partitionCount) throws Exception {
        List<Integer> partitionIndexes = new ArrayList<>(partitionCount - firstPartition);
        for (int partition = firstPartition; partition < partitionCount; partition++) {
            partitionIndexes.add(partition);
        }
        Map<Integer, Long> initialOffsets = Objects.requireNonNull(
                offsetInitializer.getInitialOffsets(metadata, partitionIndexes),
                "YT queue offset initializer returned null");
        if (initialOffsets.size() != partitionIndexes.size()
                || !initialOffsets.keySet().containsAll(partitionIndexes)) {
            throw new IllegalStateException("Incomplete YT queue offset initialization result");
        }

        List<YtQueueSplit> splits = new ArrayList<>(partitionCount - firstPartition);
        for (int partition = firstPartition; partition < partitionCount; partition++) {
            Long initialOffset = initialOffsets.get(partition);
            if (initialOffset == null) {
                throw new IllegalStateException(
                        "Missing initial offset for YT queue partition " + partition);
            }
            splits.add(new YtQueueSplit(
                    metadata.getQueueObjectId(),
                    partition,
                    initialOffset));
        }
        return splits;
    }

    private void handleInitializedSplits(
            YtQueueMetadata metadata,
            int firstPartition,
            int partitionCount,
            List<YtQueueSplit> splits,
            Throwable error) {
        if (closed) {
            return;
        }
        if (error != null) {
            initializationInProgress = false;
            throw new RuntimeException(
                    "Failed to initialize YT queue offsets for partitions " +
                            firstPartition + ".." + (partitionCount - 1),
                    error);
        }
        if (firstPartition != initializedPartitionCount
                || !Objects.equals(queueObjectId, metadata.getQueueObjectId())) {
            throw new IllegalStateException("Stale YT queue offset initialization result");
        }
        if (splits == null || splits.size() != partitionCount - firstPartition) {
            throw new IllegalStateException("Incomplete YT queue offset initialization result");
        }

        for (int index = 0; index < splits.size(); index++) {
            YtQueueSplit split = splits.get(index);
            int expectedPartition = firstPartition + index;
            if (!metadata.getQueueObjectId().equals(split.getQueueObjectId())
                    || split.getPartitionIndex() != expectedPartition) {
                throw new IllegalStateException("Invalid YT queue offset initialization result");
            }
            unassignedSplits.put(expectedPartition, split);
        }
        initializedPartitionCount = partitionCount;
        initializationInProgress = false;
        log.info("Initialized YT queue splits: {}", splits);
        assignAvailableSplits();

        if (initializedPartitionCount
                < Objects.requireNonNull(latestMetadata).getPartitionCount()) {
            YtQueueMetadata currentMetadata = Objects.requireNonNull(latestMetadata);
            startOffsetInitialization(currentMetadata);
        }
    }

    private void validateReturnedSplit(YtQueueSplit split) {
        if (queueObjectId == null || !queueObjectId.equals(split.getQueueObjectId())) {
            throw new IllegalArgumentException("Returned split belongs to a different queue object");
        }
        if (split.getPartitionIndex() >= initializedPartitionCount) {
            throw new IllegalArgumentException("Returned split partition has not been initialized");
        }
    }

    private void assignAvailableSplits() {
        if (!metadataValidated || unassignedSplits.isEmpty() || closed) {
            return;
        }
        int parallelism = context.currentParallelism();
        if (parallelism <= 0) {
            throw new IllegalStateException("Source parallelism must be positive");
        }
        Map<Integer, ReaderInfo> readers = context.registeredReaders();
        Map<Integer, List<YtQueueSplit>> assignments = new HashMap<>();
        List<Integer> assignedPartitions = new ArrayList<>();
        for (Map.Entry<Integer, YtQueueSplit> entry : unassignedSplits.entrySet()) {
            int subtaskId = Math.floorMod(entry.getKey(), parallelism);
            if (readers.containsKey(subtaskId)) {
                assignments.computeIfAbsent(subtaskId, ignored -> new ArrayList<>()).add(entry.getValue());
                assignedPartitions.add(entry.getKey());
            }
        }
        if (assignments.isEmpty()) {
            return;
        }
        context.assignSplits(new SplitsAssignment<>(assignments));
        log.info("Assigned YT queue splits: {}", assignments);
        for (Integer partition : assignedPartitions) {
            unassignedSplits.remove(partition);
        }
    }

    private static final class DiscoveryAttempt {
        private final long sequence;
        @Nullable
        private final YtQueueMetadata metadata;
        @Nullable
        private final Exception error;

        private DiscoveryAttempt(
                long sequence,
                @Nullable YtQueueMetadata metadata,
                @Nullable Exception error) {
            this.sequence = sequence;
            this.metadata = metadata;
            this.error = error;
        }

        private static DiscoveryAttempt success(long sequence, YtQueueMetadata metadata) {
            return new DiscoveryAttempt(sequence, Objects.requireNonNull(metadata), null);
        }

        private static DiscoveryAttempt failure(long sequence, Exception error) {
            return new DiscoveryAttempt(sequence, null, Objects.requireNonNull(error));
        }
    }
}
