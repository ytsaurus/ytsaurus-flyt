package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.IntToLongFunction;
import java.util.stream.Collectors;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.junit.jupiter.api.Test;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.initializer.YtQueueOffsetInitializer;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.metadata.YtQueueMetadata;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.metadata.YtQueueMetadataProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueTestFixtures.metadata;
import static tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueTestFixtures.split;

class YtQueueSplitEnumeratorTest {
    @Test
    void discoversAndAssignsOneSplitPerPartitionAndSupportsGrowth() throws Exception {
        MutableMetadataProvider metadataProvider = new MutableMetadataProvider(
                metadata("queue-object", 3));
        List<List<Integer>> initializedPartitionBatches = new ArrayList<>();
        YtQueueOffsetInitializer offsetInitializer = (metadata, partitionIndexes) -> {
            initializedPartitionBatches.add(List.copyOf(partitionIndexes));
            return offsets(partitionIndexes, partition -> 100L + partition);
        };
        TestingContext context = new TestingContext(2);
        context.registerReader(0);
        context.registerReader(1);
        YtQueueSplitEnumerator enumerator = new YtQueueSplitEnumerator(
                context, metadataProvider, offsetInitializer, 1_000);

        enumerator.start();
        context.triggerDiscovery();

        assertThat(context.assignedTo(0))
                .containsExactly(
                        split("queue-object", 0, 100),
                        split("queue-object", 2, 102));
        assertThat(context.assignedTo(1))
                .containsExactly(split("queue-object", 1, 101));
        assertThat(initializedPartitionBatches).containsExactly(List.of(0, 1, 2));

        metadataProvider.setMetadata(metadata("queue-object", 5));
        context.triggerDiscovery();

        assertThat(initializedPartitionBatches)
                .containsExactly(List.of(0, 1, 2), List.of(3, 4));
        assertThat(context.allAssignedSplits())
                .extracting(YtQueueSplit::getPartitionIndex)
                .containsExactlyInAnyOrder(0, 1, 2, 3, 4);
        YtQueueEnumeratorState checkpoint = enumerator.snapshotState(1);
        assertThat(checkpoint.getQueueObjectId()).isEqualTo("queue-object");
        assertThat(checkpoint.getInitializedPartitionCount()).isEqualTo(5);
        assertThat(checkpoint.getUnassignedSplits()).isEmpty();
    }

    @Test
    void ignoresPartitionShrinkAndRejectsQueueRecreation() {
        MutableMetadataProvider shrinkProvider = new MutableMetadataProvider(
                metadata("queue-object", 3));
        TestingContext shrinkContext = new TestingContext(1);
        YtQueueSplitEnumerator shrinkEnumerator = new YtQueueSplitEnumerator(
                shrinkContext,
                shrinkProvider,
                (metadata, partitions) -> offsets(partitions, ignored -> 0),
                1_000);
        shrinkEnumerator.start();
        shrinkContext.triggerDiscovery();
        shrinkProvider.setMetadata(metadata("queue-object", 2));

        shrinkContext.triggerDiscovery();
        assertThat(shrinkEnumerator.snapshotState(1).getInitializedPartitionCount()).isEqualTo(3);

        MutableMetadataProvider recreatedProvider = new MutableMetadataProvider(
                metadata("queue-object", 1));
        TestingContext recreatedContext = new TestingContext(1);
        YtQueueSplitEnumerator recreatedEnumerator = new YtQueueSplitEnumerator(
                recreatedContext,
                recreatedProvider,
                (metadata, partitions) -> offsets(partitions, ignored -> 0),
                1_000);
        recreatedEnumerator.start();
        recreatedContext.triggerDiscovery();
        recreatedProvider.setMetadata(metadata("replacement-object", 1));

        assertThatThrownBy(recreatedContext::triggerDiscovery)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("recreated");
    }

    @Test
    void restoreWaitsForMetadataValidationAndReturnedSplitsUseCurrentParallelism() {
        YtQueueEnumeratorState restoredState = new YtQueueEnumeratorState(
                "queue-object",
                4,
                List.of(split("queue-object", 1, 15)));
        TestingContext context = new TestingContext(1);
        context.registerReader(0);
        YtQueueSplitEnumerator enumerator = new YtQueueSplitEnumerator(
                context,
                new MutableMetadataProvider(metadata("queue-object", 4)),
                (metadata, partitions) -> offsets(partitions, ignored -> 0),
                1_000,
                restoredState);

        enumerator.start();
        assertThat(context.allAssignedSplits()).isEmpty();

        context.triggerDiscovery();
        assertThat(context.allAssignedSplits())
                .containsExactly(split("queue-object", 1, 15));

        context.clearAssignments();
        context.unregisterReader(0);
        enumerator.addSplitsBack(List.of(
                split("queue-object", 3, 30),
                split("queue-object", 3, 25)), 3);
        context.registerReader(0);
        enumerator.addReader(0);

        assertThat(context.allAssignedSplits())
                .containsExactly(split("queue-object", 3, 25));
    }

    @Test
    void restoreInitializesOffsetsOnlyForNewPartitions() {
        YtQueueEnumeratorState restoredState = new YtQueueEnumeratorState(
                "queue-object",
                2,
                List.of(split("queue-object", 1, 15)));
        List<List<Integer>> initializedPartitionBatches = new ArrayList<>();
        TestingContext context = new TestingContext(1);
        context.registerReader(0);
        YtQueueSplitEnumerator enumerator = new YtQueueSplitEnumerator(
                context,
                new MutableMetadataProvider(metadata("queue-object", 4)),
                (metadata, partitions) -> {
                    initializedPartitionBatches.add(List.copyOf(partitions));
                    return offsets(partitions, partition -> 100L + partition);
                },
                1_000,
                restoredState);

        enumerator.start();
        context.triggerDiscovery();

        assertThat(initializedPartitionBatches).containsExactly(List.of(2, 3));
        assertThat(context.allAssignedSplits()).containsExactly(
                split("queue-object", 1, 15),
                split("queue-object", 2, 102),
                split("queue-object", 3, 103));
    }

    @Test
    void initializesGrowthDiscoveredWhileOffsetInitializationIsInFlight() {
        TestingContext context = new TestingContext(1);
        context.deferOneShotCalls();
        context.registerReader(0);
        MutableMetadataProvider metadataProvider = new MutableMetadataProvider(
                metadata("queue-object", 2));
        List<List<Integer>> initializedPartitionBatches = new ArrayList<>();
        YtQueueSplitEnumerator enumerator = new YtQueueSplitEnumerator(
                context,
                metadataProvider,
                (metadata, partitions) -> {
                    initializedPartitionBatches.add(List.copyOf(partitions));
                    return offsets(partitions, partition -> 10L + partition);
                },
                1_000);

        enumerator.start();
        context.triggerDiscovery();
        metadataProvider.setMetadata(metadata("queue-object", 4));
        context.triggerDiscovery();

        YtQueueEnumeratorState inFlightState = enumerator.snapshotState(7);
        assertThat(inFlightState.getQueueObjectId()).isEqualTo("queue-object");
        assertThat(inFlightState.getInitializedPartitionCount()).isZero();
        assertThat(inFlightState.getUnassignedSplits()).isEmpty();

        context.runDeferredCalls();
        assertThat(initializedPartitionBatches)
                .containsExactly(List.of(0, 1), List.of(2, 3));
        assertThat(context.allAssignedSplits())
                .containsExactly(
                        split("queue-object", 0, 10),
                        split("queue-object", 1, 11),
                        split("queue-object", 2, 12),
                        split("queue-object", 3, 13));
    }

    @Test
    void ignoresOutOfOrderDiscoveryResults() {
        MutableMetadataProvider metadataProvider = new MutableMetadataProvider(
                metadata("queue-object", 2));
        TestingContext context = new TestingContext(1);
        YtQueueSplitEnumerator enumerator = new YtQueueSplitEnumerator(
                context,
                metadataProvider,
                (metadata, partitions) -> offsets(partitions, partition -> partition),
                1_000);
        enumerator.start();
        context.triggerDiscovery();

        metadataProvider.setMetadata(metadata("queue-object", 3));
        Runnable olderDiscovery = context.startDiscovery();
        metadataProvider.setMetadata(metadata("queue-object", 4));
        Runnable newerDiscovery = context.startDiscovery();

        newerDiscovery.run();
        olderDiscovery.run();

        assertThat(enumerator.snapshotState(1).getInitializedPartitionCount()).isEqualTo(4);
    }

    @Test
    void rejectsNegativeOffsetFromInitializer() {
        TestingContext context = new TestingContext(1);
        YtQueueSplitEnumerator enumerator = new YtQueueSplitEnumerator(
                context,
                new MutableMetadataProvider(metadata("queue-object", 1)),
                (metadata, partitions) -> offsets(partitions, ignored -> -1),
                1_000);
        enumerator.start();

        assertThatThrownBy(context::triggerDiscovery)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("initialize YT queue offsets")
                .hasRootCauseInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsIncompleteInitialOffsetMap() {
        TestingContext context = new TestingContext(1);
        YtQueueSplitEnumerator enumerator = new YtQueueSplitEnumerator(
                context,
                new MutableMetadataProvider(metadata("queue-object", 2)),
                (metadata, partitions) -> Map.of(0, 10L),
                1_000);
        enumerator.start();

        assertThatThrownBy(context::triggerDiscovery)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("initialize YT queue offsets")
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("Incomplete YT queue offset initialization result");
    }

    @Test
    void closeClosesDependenciesOnce() throws Exception {
        CloseTrackingMetadataProvider metadataProvider = new CloseTrackingMetadataProvider(null);
        CloseTrackingOffsetInitializer offsetInitializer = new CloseTrackingOffsetInitializer(null);
        YtQueueSplitEnumerator enumerator = new YtQueueSplitEnumerator(
                new TestingContext(1),
                metadataProvider,
                offsetInitializer,
                1_000);

        enumerator.close();
        enumerator.close();

        assertThat(metadataProvider.closeCalls).hasValue(1);
        assertThat(offsetInitializer.closeCalls).hasValue(1);
    }

    @Test
    void closePreservesPrimaryFailureAndSuppressesSecondary() {
        IOException metadataFailure = new IOException("metadata close failed");
        IOException offsetFailure = new IOException("offset initializer close failed");
        CloseTrackingMetadataProvider metadataProvider = new CloseTrackingMetadataProvider(metadataFailure);
        CloseTrackingOffsetInitializer offsetInitializer = new CloseTrackingOffsetInitializer(offsetFailure);
        YtQueueSplitEnumerator enumerator = new YtQueueSplitEnumerator(
                new TestingContext(1),
                metadataProvider,
                offsetInitializer,
                1_000);

        IOException thrown = assertThrows(IOException.class, enumerator::close);

        assertThat(thrown).isSameAs(metadataFailure);
        assertThat(thrown.getSuppressed()).containsExactly(offsetFailure);
        assertThat(metadataProvider.closeCalls).hasValue(1);
        assertThat(offsetInitializer.closeCalls).hasValue(1);
    }

    private static Map<Integer, Long> offsets(
            List<Integer> partitionIndexes,
            IntToLongFunction offsetFunction) {
        Map<Integer, Long> offsets = new LinkedHashMap<>(partitionIndexes.size());
        for (Integer partitionIndex : partitionIndexes) {
            offsets.put(partitionIndex, offsetFunction.applyAsLong(partitionIndex));
        }
        return offsets;
    }

    private static final class MutableMetadataProvider implements YtQueueMetadataProvider {
        private YtQueueMetadata metadata;

        private MutableMetadataProvider(YtQueueMetadata metadata) {
            this.metadata = metadata;
        }

        private void setMetadata(YtQueueMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public YtQueueMetadata getMetadata() {
            return metadata;
        }
    }

    private static final class CloseTrackingMetadataProvider implements YtQueueMetadataProvider {
        private final IOException closeFailure;

        private final AtomicInteger closeCalls = new AtomicInteger();

        private CloseTrackingMetadataProvider(IOException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public YtQueueMetadata getMetadata() {
            return metadata("queue-object", 0);
        }

        @Override
        public void close() throws IOException {
            closeCalls.incrementAndGet();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static final class CloseTrackingOffsetInitializer implements YtQueueOffsetInitializer {
        private final IOException closeFailure;

        private final AtomicInteger closeCalls = new AtomicInteger();

        private CloseTrackingOffsetInitializer(IOException closeFailure) {
            this.closeFailure = closeFailure;
        }

        @Override
        public Map<Integer, Long> getInitialOffsets(
                YtQueueMetadata metadata,
                List<Integer> partitionIndexes) {
            return offsets(partitionIndexes, ignored -> 0);
        }

        @Override
        public void close() throws IOException {
            closeCalls.incrementAndGet();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }

    private static final class TestingContext implements SplitEnumeratorContext<YtQueueSplit> {
        private final Map<Integer, ReaderInfo> readers = new HashMap<>();
        private final List<SplitsAssignment<YtQueueSplit>> assignments = new ArrayList<>();
        private final List<Runnable> deferredCalls = new ArrayList<>();
        private final int parallelism;
        private Callable<?> discoveryCallable;
        private BiConsumer<Object, Throwable> discoveryHandler;
        private boolean deferOneShotCalls;

        private TestingContext(int parallelism) {
            this.parallelism = parallelism;
        }

        private void registerReader(int subtaskId) {
            readers.put(subtaskId, new ReaderInfo(subtaskId, "reader-" + subtaskId));
        }

        private void unregisterReader(int subtaskId) {
            readers.remove(subtaskId);
        }

        private void deferOneShotCalls() {
            deferOneShotCalls = true;
        }

        private void triggerDiscovery() {
            startDiscovery().run();
        }

        private Runnable startDiscovery() {
            if (discoveryCallable == null) {
                throw new IllegalStateException("No periodic discovery call registered");
            }
            Object result;
            Throwable error = null;
            try {
                result = discoveryCallable.call();
            } catch (Throwable caught) {
                result = null;
                error = caught;
            }
            Object completedResult = result;
            Throwable completedError = error;
            return () -> discoveryHandler.accept(completedResult, completedError);
        }

        private void runDeferredCalls() {
            List<Runnable> calls = new ArrayList<>(deferredCalls);
            deferredCalls.clear();
            deferOneShotCalls = false;
            calls.forEach(Runnable::run);
        }

        private List<YtQueueSplit> assignedTo(int subtaskId) {
            return assignments.stream()
                    .flatMap(assignment -> assignment.assignment()
                            .getOrDefault(subtaskId, List.of())
                            .stream())
                    .collect(Collectors.toList());
        }

        private List<YtQueueSplit> allAssignedSplits() {
            return assignments.stream()
                    .flatMap(assignment -> assignment.assignment().values().stream())
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
        }

        private void clearAssignments() {
            assignments.clear();
        }

        @Override
        public SplitEnumeratorMetricGroup metricGroup() {
            return null;
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        }

        @Override
        public int currentParallelism() {
            return parallelism;
        }

        @Override
        public Map<Integer, ReaderInfo> registeredReaders() {
            return readers;
        }

        @Override
        public void assignSplits(SplitsAssignment<YtQueueSplit> newSplitAssignments) {
            assignments.add(newSplitAssignments);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
        }

        @Override
        public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
            Runnable call = () -> execute(callable, handler);
            if (deferOneShotCalls) {
                deferredCalls.add(call);
            } else {
                call.run();
            }
        }

        @Override
        public <T> void callAsync(
                Callable<T> callable,
                BiConsumer<T, Throwable> handler,
                long initialDelayMillis,
                long periodMillis) {
            discoveryCallable = callable;
            discoveryHandler = castHandler(handler);
        }

        @Override
        public void runInCoordinatorThread(Runnable runnable) {
            runnable.run();
        }

        private static <T> void execute(Callable<T> callable, BiConsumer<T, Throwable> handler) {
            T result;
            try {
                result = callable.call();
            } catch (Throwable error) {
                handler.accept(null, error);
                return;
            }
            handler.accept(result, null);
        }

        @SuppressWarnings("unchecked")
        private static <T> BiConsumer<Object, Throwable> castHandler(BiConsumer<T, Throwable> handler) {
            return (BiConsumer<Object, Throwable>) (BiConsumer<?, Throwable>) handler;
        }
    }
}
