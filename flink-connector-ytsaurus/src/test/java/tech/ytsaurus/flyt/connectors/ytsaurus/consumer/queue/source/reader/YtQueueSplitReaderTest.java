package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsRemoval;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueTrimmedOffsetPolicy;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueReaderOptions;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueueBatch;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueuePullRequest;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueueRawRecord;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueTestFixtures.split;

class YtQueueSplitReaderTest {
    private static final Duration SHORT_BACKOFF = Duration.ofMillis(10);

    private static final int ASYNC_TIMEOUT_SECONDS = 5;

    private final ExecutorService fetchExecutor = Executors.newCachedThreadPool();

    private final List<YtQueueSplitReader> readers = new ArrayList<>();

    @AfterEach
    void tearDown() throws Exception {
        Exception closeFailure = null;
        for (YtQueueSplitReader reader : readers) {
            try {
                reader.close();
            } catch (Exception e) {
                if (closeFailure == null) {
                    closeFailure = e;
                } else {
                    closeFailure.addSuppressed(e);
                }
            }
        }
        fetchExecutor.shutdownNow();
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    @Test
    void returnsWholeBatchWithAbsoluteOffsets() throws Exception {
        TableSchema schema = queueSchema();
        UnversionedRow first = row();
        UnversionedRow second = row();
        AtomicInteger calls = new AtomicInteger();
        TestingPuller puller = new TestingPuller(request -> {
            if (calls.getAndIncrement() == 0) {
                return completedBatch(schema, request.getOffset(), first, second);
            }
            return new CompletableFuture<>();
        });
        YtQueueSplit queueSplit = split("queue-id", 4, 7);
        YtQueueSplitReader reader = reader(() -> puller, SHORT_BACKOFF, 1, 2);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(queueSplit)));

        RecordsWithSplitIds<YtQueueRawRecord> records = reader.fetch();

        assertThat(records.nextSplit()).isEqualTo(queueSplit.splitId());
        YtQueueRawRecord firstRecord = records.nextRecordFromSplit();
        YtQueueRawRecord secondRecord = records.nextRecordFromSplit();
        assertThat(firstRecord.getPartitionIndex()).isEqualTo(4);
        assertThat(firstRecord.getOffset()).isEqualTo(7);
        assertThat(firstRecord.getRow()).isSameAs(first);
        assertThat(secondRecord.getPartitionIndex()).isEqualTo(4);
        assertThat(secondRecord.getOffset()).isEqualTo(8);
        assertThat(secondRecord.getRow()).isSameAs(second);
        assertThat(records.nextRecordFromSplit()).isNull();
        assertThat(records.nextSplit()).isNull();
        assertThat(records.finishedSplits()).isEmpty();
    }

    @Test
    void continuesPullingFromBatchFinishOffset() throws Exception {
        TableSchema schema = queueSchema();
        YtQueueBatch firstBatch = batch(schema, 1, row(), row(), row());
        CountDownLatch secondPull = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        TestingPuller puller = new TestingPuller(request -> {
            if (calls.getAndIncrement() == 0) {
                return CompletableFuture.completedFuture(firstBatch);
            }
            secondPull.countDown();
            return new CompletableFuture<>();
        });
        YtQueueSplit queueSplit = split("queue-id", 0, 1);
        YtQueueSplitReader reader = reader(() -> puller, SHORT_BACKOFF, 1, 2);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(queueSplit)));

        assertThat(secondPull.await(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(puller.requests.stream()
                .limit(2)
                .map(YtQueuePullRequest::getOffset))
                .containsExactly(1L, 4L);

        RecordsWithSplitIds<YtQueueRawRecord> records = reader.fetch();
        assertThat(records.nextSplit()).isEqualTo(queueSplit.splitId());
        List<Long> offsets = new ArrayList<>();
        YtQueueRawRecord record;
        while ((record = records.nextRecordFromSplit()) != null) {
            offsets.add(record.getOffset());
        }
        assertThat(offsets).containsExactly(1L, 2L, 3L);
    }

    @Test
    void skipsBatchGapWhenRequestedOffsetIsTrimmed() throws Exception {
        TableSchema schema = queueSchema();
        CountDownLatch secondPull = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        TestingPuller puller = new TestingPuller(request -> {
            if (calls.getAndIncrement() == 0) {
                return CompletableFuture.completedFuture(batch(schema, 5, row()));
            }
            secondPull.countDown();
            return new CompletableFuture<>();
        });
        YtQueueSplit queueSplit = split("queue-id", 0, 4);
        YtQueueSplitReader reader = reader(
                () -> puller,
                SHORT_BACKOFF,
                1,
                2,
                YtQueueTrimmedOffsetPolicy.SKIP);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(queueSplit)));

        assertThat(secondPull.await(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(puller.requests.stream()
                .limit(2)
                .map(YtQueuePullRequest::getOffset))
                .containsExactly(4L, 6L);
        RecordsWithSplitIds<YtQueueRawRecord> records = reader.fetch();
        assertThat(records.nextSplit()).isEqualTo(queueSplit.splitId());
        assertThat(records.nextRecordFromSplit().getOffset()).isEqualTo(5);
    }

    @Test
    void failsWhenRequestedOffsetIsTrimmed() {
        TableSchema schema = queueSchema();
        TestingPuller puller = new TestingPuller(request -> CompletableFuture.completedFuture(
                batch(schema, 5, row())));
        YtQueueSplitReader reader = reader(
                () -> puller,
                SHORT_BACKOFF,
                1,
                2,
                YtQueueTrimmedOffsetPolicy.FAIL);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(split("queue-id", 0, 4))));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessage("Queue partition 0 returned start offset 5 for requested offset 4");
    }

    @Test
    void rejectsBatchStartingBeforeRequestedOffsetEvenWithSkipPolicy() {
        TableSchema schema = queueSchema();
        TestingPuller puller = new TestingPuller(request -> CompletableFuture.completedFuture(
                batch(schema, 4, row())));
        YtQueueSplitReader reader = reader(
                () -> puller,
                SHORT_BACKOFF,
                1,
                2,
                YtQueueTrimmedOffsetPolicy.SKIP);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(split("queue-id", 0, 5))));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessage("Queue partition 0 returned start offset 4 for requested offset 5");
    }

    @Test
    void limitsWorkerCountByAssignedPartitionCount() throws Exception {
        CountDownLatch pullsStarted = new CountDownLatch(2);
        TestingPullerFactory pullers = new TestingPullerFactory(request -> {
            pullsStarted.countDown();
            return new CompletableFuture<>();
        });
        YtQueueSplitReader reader = reader(pullers, SHORT_BACKOFF, 5, 5);

        reader.handleSplitsChanges(new SplitsAddition<>(List.of(
                split("queue-id", 0, 0),
                split("queue-id", 1, 0)
        )));

        assertThat(pullsStarted.await(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(reader.getWorkerCount()).isEqualTo(2);
        assertThat(pullers.createdPullers()).hasSize(2);
    }

    @Test
    void pullsDifferentPartitionsConcurrently() throws Exception {
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        CountDownLatch concurrentPulls = new CountDownLatch(2);
        ConcurrentLinkedQueue<Integer> partitions = new ConcurrentLinkedQueue<>();
        TestingPullerFactory pullers = new TestingPullerFactory(request -> {
            partitions.add(request.getPartitionIndex());
            int currentInFlight = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(currentInFlight, Math::max);
            concurrentPulls.countDown();
            CompletableFuture<YtQueueBatch> result = new CompletableFuture<>();
            result.whenComplete((ignored, error) -> inFlight.decrementAndGet());
            return result;
        });
        YtQueueSplitReader reader = reader(pullers, SHORT_BACKOFF, 2, 2);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(
                split("queue-id", 0, 0),
                split("queue-id", 1, 0)
        )));

        assertThat(concurrentPulls.await(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(maxInFlight).hasValue(2);
        assertThat(partitions).containsExactlyInAnyOrder(0, 1);
    }

    @Test
    void appliesBackpressureWhenBatchBufferIsFull() throws Exception {
        TableSchema schema = queueSchema();
        CountDownLatch thirdPull = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        TestingPuller puller = new TestingPuller(request -> {
            int call = calls.getAndIncrement();
            if (call < 3) {
                if (call == 2) {
                    thirdPull.countDown();
                }
                return completedBatch(schema, request.getOffset(), row());
            }
            return new CompletableFuture<>();
        });
        YtQueueSplit queueSplit = split("queue-id", 0, 0);
        YtQueueSplitReader reader = reader(() -> puller, SHORT_BACKOFF, 2, 2);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(queueSplit)));

        assertThat(thirdPull.await(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        awaitCondition(() -> reader.getBufferedBatchCount() == 2);
        assertThat(puller.requests.stream()
                .limit(3)
                .map(YtQueuePullRequest::getOffset))
                .containsExactly(0L, 1L, 2L);

        RecordsWithSplitIds<YtQueueRawRecord> firstBatch = reader.fetch();
        assertThat(firstBatch.nextSplit()).isEqualTo(queueSplit.splitId());
        assertThat(firstBatch.nextRecordFromSplit().getOffset()).isZero();
        awaitCondition(() -> reader.getBufferedBatchCount() == 2);
        RecordsWithSplitIds<YtQueueRawRecord> secondBatch = reader.fetch();
        assertThat(secondBatch.nextSplit()).isEqualTo(queueSplit.splitId());
        assertThat(secondBatch.nextRecordFromSplit().getOffset()).isEqualTo(1);
        RecordsWithSplitIds<YtQueueRawRecord> thirdBatch = reader.fetch();
        assertThat(thirdBatch.nextSplit()).isEqualTo(queueSplit.splitId());
        assertThat(thirdBatch.nextRecordFromSplit().getOffset()).isEqualTo(2);
        assertThat(reader.getBufferedBatchCount()).isZero();
    }

    @Test
    void doesNotOverlapPullsForPartitionAndAdvancesOffsetsInOrder() throws Exception {
        TableSchema schema = queueSchema();
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        CountDownLatch firstPull = new CountDownLatch(1);
        CountDownLatch secondPull = new CountDownLatch(1);
        CountDownLatch thirdPull = new CountDownLatch(1);
        AtomicInteger calls = new AtomicInteger();
        AtomicBoolean pullStartedBeforePreviousCompletion = new AtomicBoolean();
        CopyOnWriteArrayList<CompletableFuture<YtQueueBatch>> batches = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<CountDownLatch> batchCompletions = new CopyOnWriteArrayList<>();
        TestingPuller puller = new TestingPuller(request -> {
            int call = calls.getAndIncrement();
            if (call > 0 && !batches.get(call - 1).isDone()) {
                pullStartedBeforePreviousCompletion.set(true);
            }
            int currentInFlight = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(currentInFlight, Math::max);
            CompletableFuture<YtQueueBatch> batch = new CompletableFuture<>();
            CountDownLatch batchCompletion = new CountDownLatch(1);
            batch.whenComplete((ignored, error) -> {
                inFlight.decrementAndGet();
                batchCompletion.countDown();
            });
            batches.add(batch);
            batchCompletions.add(batchCompletion);
            if (call == 0) {
                firstPull.countDown();
            } else if (call == 1) {
                secondPull.countDown();
            } else if (call == 2) {
                thirdPull.countDown();
            }
            return batch;
        });
        YtQueueSplit queueSplit = split("queue-id", 0, 7);
        YtQueueSplitReader reader = reader(() -> puller, SHORT_BACKOFF, 2, 3);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(queueSplit)));

        assertThat(firstPull.await(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        batches.get(0).complete(batch(schema, 7, row()));
        assertThat(batchCompletions.get(0).await(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(secondPull.await(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        batches.get(1).complete(batch(schema, 8, row()));
        assertThat(batchCompletions.get(1).await(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(thirdPull.await(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        batches.get(2).complete(batch(schema, 9, row()));
        assertThat(batchCompletions.get(2).await(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        awaitCondition(() -> reader.getBufferedBatchCount() == 3);

        List<Long> requestedOffsets = puller.requests.stream()
                .limit(3)
                .map(YtQueuePullRequest::getOffset)
                .collect(Collectors.toList());
        assertThat(requestedOffsets).containsExactly(7L, 8L, 9L);
        assertThat(pullStartedBeforePreviousCompletion).isFalse();
        assertThat(maxInFlight).hasValue(1);

        for (long expectedOffset = 7; expectedOffset <= 9; expectedOffset++) {
            RecordsWithSplitIds<YtQueueRawRecord> records = reader.fetch();
            assertThat(records.nextSplit()).isEqualTo(queueSplit.splitId());
            assertThat(records.nextRecordFromSplit().getOffset()).isEqualTo(expectedOffset);
        }
    }

    @Test
    void emptyBatchBackoffDoesNotBlockAnotherPartition() throws Exception {
        TableSchema schema = queueSchema();
        ConcurrentHashMap<Integer, AtomicInteger> callsByPartition = new ConcurrentHashMap<>();
        TestingPuller puller = new TestingPuller(request -> {
            int call = callsByPartition
                    .computeIfAbsent(request.getPartitionIndex(), ignored -> new AtomicInteger())
                    .getAndIncrement();
            if (call > 0) {
                return new CompletableFuture<>();
            }
            if (request.getPartitionIndex() == 0) {
                return completedBatch(schema, request.getOffset());
            }
            return completedBatch(schema, request.getOffset(), row());
        });
        YtQueueSplit secondSplit = split("queue-id", 1, 9);
        YtQueueSplitReader reader = reader(() -> puller, Duration.ofSeconds(5), 1, 2);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(
                split("queue-id", 0, 0),
                secondSplit
        )));

        RecordsWithSplitIds<YtQueueRawRecord> records = reader.fetch();

        assertThat(records.nextSplit()).isEqualTo(secondSplit.splitId());
        assertThat(records.nextRecordFromSplit().getOffset()).isEqualTo(9);
        assertThat(puller.requests.stream()
                .limit(2)
                .map(YtQueuePullRequest::getPartitionIndex))
                .containsExactly(0, 1);
    }

    @Test
    void propagatesWorkerFailureFromFetch() {
        IllegalStateException workerFailure = new IllegalStateException("pull failed");
        TestingPuller puller = new TestingPuller(request -> CompletableFuture.failedFuture(workerFailure));
        YtQueueSplitReader reader = reader(() -> puller, SHORT_BACKOFF, 1, 2);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(split("queue-id", 0, 0))));

        assertThatThrownBy(reader::fetch)
                .isInstanceOf(IOException.class)
                .hasMessage("Queue reader worker failed")
                .hasRootCauseMessage("pull failed");
    }

    @Test
    void wakeUpUnblocksFetchWithoutCancellingWorkerPull() throws Exception {
        CountDownLatch pullStarted = new CountDownLatch(1);
        CompletableFuture<YtQueueBatch> pendingPull = new CompletableFuture<>();
        TestingPuller puller = new TestingPuller(request -> {
            pullStarted.countDown();
            return pendingPull;
        });
        YtQueueSplitReader reader = reader(() -> puller, SHORT_BACKOFF, 1, 2);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(split("queue-id", 0, 0))));
        CompletableFuture<RecordsWithSplitIds<YtQueueRawRecord>> fetch = fetchAsync(reader);
        assertThat(pullStarted.await(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

        reader.wakeUp();

        RecordsWithSplitIds<YtQueueRawRecord> records = fetch.get(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(records.nextSplit()).isNull();
        assertThat(puller.wakeUpCalls).hasValue(0);
        assertThat(pendingPull).isNotCancelled();
    }

    @Test
    void closeCancelsInflightPullsAndClosesEveryCreatedPuller() throws Exception {
        CountDownLatch pullsStarted = new CountDownLatch(3);
        TestingPullerFactory pullers = new TestingPullerFactory(request -> {
            pullsStarted.countDown();
            return new CompletableFuture<>();
        });
        YtQueueSplitReader reader = reader(pullers, SHORT_BACKOFF, 3, 3);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(
                split("queue-id", 0, 0),
                split("queue-id", 1, 0),
                split("queue-id", 2, 0)
        )));
        assertThat(pullsStarted.await(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

        reader.close();

        assertThat(pullers.createdPullers()).hasSize(3);
        assertThat(pullers.createdPullers())
                .allSatisfy(puller -> {
                    assertThat(puller.closed).isTrue();
                    assertThat(puller.wakeUpCalls.get()).isGreaterThanOrEqualTo(1);
                    assertThat(puller.createdFutures)
                            .allSatisfy(future -> assertThat(future).isCancelled());
                    assertThat(puller.inFlightFutures).isEmpty();
                });
    }

    @Test
    void closeUnblocksFetchWithoutAssignedSplits() throws Exception {
        YtQueueSplitReader reader = reader(
                () -> new TestingPuller(request -> new CompletableFuture<>()),
                SHORT_BACKOFF,
                1,
                1);
        CompletableFuture<RecordsWithSplitIds<YtQueueRawRecord>> fetch = fetchAsync(reader);

        reader.close();

        assertThat(fetch.get(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS).nextSplit()).isNull();
    }

    @Test
    void splitRemovalDropsBufferedBatchAndReportsFinishedSplit() throws Exception {
        TableSchema schema = queueSchema();
        AtomicInteger calls = new AtomicInteger();
        TestingPuller puller = new TestingPuller(request -> {
            if (calls.getAndIncrement() == 0) {
                return completedBatch(schema, request.getOffset(), row());
            }
            return new CompletableFuture<>();
        });
        YtQueueSplit queueSplit = split("queue-id", 0, 0);
        YtQueueSplitReader reader = reader(() -> puller, SHORT_BACKOFF, 1, 2);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(queueSplit)));
        awaitCondition(() -> reader.getBufferedBatchCount() == 1);

        reader.handleSplitsChanges(new SplitsRemoval<>(List.of(queueSplit)));
        RecordsWithSplitIds<YtQueueRawRecord> records = reader.fetch();

        assertThat(reader.getBufferedBatchCount()).isZero();
        assertThat(records.nextSplit()).isNull();
        assertThat(records.finishedSplits()).containsExactly(queueSplit.splitId());
    }

    @Test
    void remainingPartitionKeepsReadingAfterWorkerPoolShrinks() throws Exception {
        TableSchema schema = queueSchema();
        CountDownLatch initialPulls = new CountDownLatch(2);
        ConcurrentHashMap<Integer, CompletableFuture<YtQueueBatch>> firstPulls = new ConcurrentHashMap<>();
        ConcurrentHashMap<Integer, AtomicInteger> callsByPartition = new ConcurrentHashMap<>();
        TestingPullerFactory pullers = new TestingPullerFactory(request -> {
            int call = callsByPartition
                    .computeIfAbsent(request.getPartitionIndex(), ignored -> new AtomicInteger())
                    .getAndIncrement();
            if (call == 0) {
                CompletableFuture<YtQueueBatch> firstPull = new CompletableFuture<>();
                firstPulls.put(request.getPartitionIndex(), firstPull);
                initialPulls.countDown();
                return firstPull;
            }
            return completedBatch(schema, request.getOffset(), row());
        });
        YtQueueSplit removedSplit = split("queue-id", 0, 0);
        YtQueueSplit remainingSplit = split("queue-id", 1, 10);
        YtQueueSplitReader reader = reader(pullers, SHORT_BACKOFF, 2, 2);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(removedSplit, remainingSplit)));
        assertThat(initialPulls.await(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

        reader.handleSplitsChanges(new SplitsRemoval<>(List.of(removedSplit)));
        firstPulls.get(1).complete(batch(schema, 10, row()));

        RecordsWithSplitIds<YtQueueRawRecord> finished = reader.fetch();
        assertThat(finished.finishedSplits()).containsExactly(removedSplit.splitId());
        RecordsWithSplitIds<YtQueueRawRecord> records = reader.fetch();
        assertThat(records.nextSplit()).isEqualTo(remainingSplit.splitId());
        assertThat(records.nextRecordFromSplit().getOffset()).isEqualTo(10);
        assertThat(reader.getWorkerCount()).isEqualTo(1);
    }

    @Test
    void pauseStopsNewPullsUntilSplitIsResumed() throws Exception {
        TableSchema schema = queueSchema();
        CompletableFuture<YtQueueBatch> firstBatch = new CompletableFuture<>();
        CompletableFuture<YtQueueBatch> secondBatch = new CompletableFuture<>();
        CompletableFuture<YtQueueBatch> barrierBatch = new CompletableFuture<>();
        CountDownLatch firstPull = new CountDownLatch(1);
        CountDownLatch secondPull = new CountDownLatch(1);
        CountDownLatch barrierPull = new CountDownLatch(1);
        AtomicInteger queuePulls = new AtomicInteger();
        AtomicInteger barrierPulls = new AtomicInteger();
        TestingPuller puller = new TestingPuller(request -> {
            if (request.getPartitionIndex() == 1) {
                if (barrierPulls.getAndIncrement() == 0) {
                    barrierPull.countDown();
                    return barrierBatch;
                }
                return new CompletableFuture<>();
            }
            int call = queuePulls.getAndIncrement();
            if (call == 0) {
                firstPull.countDown();
                return firstBatch;
            }
            if (call == 1) {
                secondPull.countDown();
                return secondBatch;
            }
            return new CompletableFuture<>();
        });
        YtQueueSplit queueSplit = split("queue-id", 0, 5);
        YtQueueSplit barrierSplit = split("queue-id", 1, 0);
        YtQueueSplitReader reader = reader(() -> puller, SHORT_BACKOFF, 1, 2);
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(queueSplit)));
        assertThat(firstPull.await(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

        reader.pauseOrResumeSplits(List.of(queueSplit), List.of());
        reader.handleSplitsChanges(new SplitsAddition<>(List.of(barrierSplit)));
        firstBatch.complete(batch(schema, 5, row()));
        RecordsWithSplitIds<YtQueueRawRecord> firstResult = reader.fetch();
        assertThat(firstResult.nextSplit()).isEqualTo(queueSplit.splitId());
        assertThat(firstResult.nextRecordFromSplit().getOffset()).isEqualTo(5);
        assertThat(barrierPull.await(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(puller.requests.stream()
                .limit(2)
                .map(YtQueuePullRequest::getPartitionIndex))
                .containsExactly(0, 1);

        reader.pauseOrResumeSplits(List.of(), List.of(queueSplit));
        barrierBatch.complete(batch(schema, 0));
        assertThat(secondPull.await(ASYNC_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(puller.requests.get(2).getOffset()).isEqualTo(6);
        secondBatch.complete(batch(schema, 6, row()));
        RecordsWithSplitIds<YtQueueRawRecord> secondResult = reader.fetch();
        assertThat(secondResult.nextSplit()).isEqualTo(queueSplit.splitId());
        assertThat(secondResult.nextRecordFromSplit().getOffset()).isEqualTo(6);
    }

    private YtQueueSplitReader reader(
            Supplier<? extends YtQueuePuller> pullerSupplier,
            Duration backoff,
            int workerCount,
            int bufferCapacity) {
        return reader(
                pullerSupplier,
                backoff,
                workerCount,
                bufferCapacity,
                YtQueueTrimmedOffsetPolicy.FAIL);
    }

    private YtQueueSplitReader reader(
            Supplier<? extends YtQueuePuller> pullerSupplier,
            Duration backoff,
            int workerCount,
            int bufferCapacity,
            YtQueueTrimmedOffsetPolicy trimmedOffsetPolicy) {
        YtQueueSplitReader reader = new YtQueueSplitReader(
                pullerSupplier,
                new YtQueueReaderOptions(100, 1024, backoff, workerCount, bufferCapacity),
                trimmedOffsetPolicy
        );
        readers.add(reader);
        return reader;
    }

    private CompletableFuture<RecordsWithSplitIds<YtQueueRawRecord>> fetchAsync(YtQueueSplitReader reader) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return reader.fetch();
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        }, fetchExecutor);
    }

    private static void awaitCondition(BooleanSupplier condition) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(ASYNC_TIMEOUT_SECONDS);
        while (!condition.getAsBoolean() && System.nanoTime() < deadlineNanos) {
            TimeUnit.MILLISECONDS.sleep(5);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }

    private static CompletableFuture<YtQueueBatch> completedBatch(
            TableSchema schema,
            long startOffset,
            UnversionedRow... rows) {
        return CompletableFuture.completedFuture(batch(schema, startOffset, rows));
    }

    private static TableSchema queueSchema() {
        return TableSchema.builder().build();
    }

    private static YtQueueBatch batch(
            TableSchema schema,
            long startOffset,
            UnversionedRow... rows) {
        return new YtQueueBatch(schema, startOffset, List.of(rows));
    }

    private static UnversionedRow row() {
        return new UnversionedRow(List.of());
    }

    private static final class TestingPullerFactory implements Supplier<YtQueuePuller> {
        private final Function<YtQueuePullRequest, CompletableFuture<YtQueueBatch>> response;

        private final CopyOnWriteArrayList<TestingPuller> pullers = new CopyOnWriteArrayList<>();

        private TestingPullerFactory(Function<YtQueuePullRequest, CompletableFuture<YtQueueBatch>> response) {
            this.response = response;
        }

        @Override
        public YtQueuePuller get() {
            TestingPuller puller = new TestingPuller(response);
            pullers.add(puller);
            return puller;
        }

        private List<TestingPuller> createdPullers() {
            return List.copyOf(pullers);
        }
    }

    private static final class TestingPuller implements YtQueuePuller {
        private final Function<YtQueuePullRequest, CompletableFuture<YtQueueBatch>> response;

        private final CopyOnWriteArrayList<YtQueuePullRequest> requests = new CopyOnWriteArrayList<>();

        private final CopyOnWriteArrayList<CompletableFuture<YtQueueBatch>> inFlightFutures =
                new CopyOnWriteArrayList<>();

        private final CopyOnWriteArrayList<CompletableFuture<YtQueueBatch>> createdFutures =
                new CopyOnWriteArrayList<>();

        private final AtomicInteger wakeUpCalls = new AtomicInteger();

        private final AtomicBoolean closed = new AtomicBoolean();

        private TestingPuller(Function<YtQueuePullRequest, CompletableFuture<YtQueueBatch>> response) {
            this.response = response;
        }

        @Override
        public CompletableFuture<YtQueueBatch> pull(YtQueuePullRequest request) {
            requests.add(request);
            CompletableFuture<YtQueueBatch> future = Objects.requireNonNull(response.apply(request));
            createdFutures.add(future);
            inFlightFutures.add(future);
            future.whenComplete((ignored, error) -> inFlightFutures.remove(future));
            return future;
        }

        @Override
        public void wakeUp() {
            wakeUpCalls.incrementAndGet();
            inFlightFutures.forEach(future -> future.cancel(true));
        }

        @Override
        public void close() {
            closed.set(true);
            wakeUp();
        }
    }
}
