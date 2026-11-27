package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.flink.metrics.Counter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import tech.ytsaurus.client.ApiServiceClient;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.AdvanceConsumer;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.request.TransactionType;
import tech.ytsaurus.core.cypress.YPath;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueTestFixtures.split;

@Timeout(10)
class YTsaurusQueueConsumerOffsetCommitterTest {
    private static final String CONSUMER_PATH = "//home/test/consumer";

    private static final String QUEUE_PATH = "//home/test/queue";

    private final Counter commitsSucceeded = mock(Counter.class);

    private final Counter commitsFailed = mock(Counter.class);

    @Test
    void commitsSnapshotOffsetsOnlyAfterCheckpointCompletion() {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = successfulTransaction(client);
        try (YTsaurusQueueConsumerOffsetCommitter committer = committer(client)) {
            List<YtQueueSplit> offsets = new ArrayList<>(List.of(
                    split("queue-id", 2, 23),
                    split("queue-id", 0, 17)));
            committer.snapshotState(11, offsets);
            offsets.clear();
            verifyNoInteractions(client, transaction);

            committer.notifyCheckpointComplete(11);
            verify(transaction, timeout(2_000)).close();
        }

        ArgumentCaptor<StartTransaction> startRequest = ArgumentCaptor.forClass(StartTransaction.class);
        verify(client).startTransaction(startRequest.capture());
        assertThat(startRequest.getValue().getType()).isEqualTo(TransactionType.Tablet);
        ArgumentCaptor<AdvanceConsumer> advances = ArgumentCaptor.forClass(AdvanceConsumer.class);
        verify(transaction, times(2)).advanceConsumer(advances.capture());
        assertAdvance(advances.getAllValues().get(0), 0, 17);
        assertAdvance(advances.getAllValues().get(1), 2, 23);
        verify(transaction).commit();
        verify(commitsSucceeded).inc();
        verifyNoInteractions(commitsFailed);
    }

    @Test
    void laterCompletedCheckpointSubsumesOlderSnapshots() {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = successfulTransaction(client);
        try (YTsaurusQueueConsumerOffsetCommitter committer = committer(client)) {
            committer.snapshotState(1, List.of(split("queue-id", 0, 10)));
            committer.snapshotState(2, List.of(split("queue-id", 0, 20)));

            committer.notifyCheckpointComplete(2);
            committer.notifyCheckpointComplete(1);
            committer.notifyCheckpointAborted(1);
            committer.notifyCheckpointComplete(2);
            verify(transaction, timeout(2_000)).close();
        }

        ArgumentCaptor<AdvanceConsumer> request = ArgumentCaptor.forClass(AdvanceConsumer.class);
        verify(transaction).advanceConsumer(request.capture());
        assertAdvance(request.getValue(), 0, 20);
        verify(client).startTransaction(any(StartTransaction.class));
    }

    @Test
    void advancesOffsetsInParallelAndWaitsForAllBeforeCommit() {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = successfulTransaction(client);
        CompletableFuture<ApiServiceTransaction> start = new CompletableFuture<>();
        CompletableFuture<Void> firstAdvance = new CompletableFuture<>();
        CompletableFuture<Void> secondAdvance = new CompletableFuture<>();
        CompletableFuture<Void> commit = new CompletableFuture<>();
        when(client.startTransaction(any(StartTransaction.class))).thenReturn(start);
        when(transaction.advanceConsumer(any(AdvanceConsumer.class))).thenReturn(firstAdvance, secondAdvance);
        when(transaction.commit()).thenReturn(commit);
        try (YTsaurusQueueConsumerOffsetCommitter committer = committer(client)) {
            committer.snapshotState(1, List.of(split("queue-id", 0, 10), split("queue-id", 1, 20)));
            committer.notifyCheckpointComplete(1);
            verify(client, timeout(2_000)).startTransaction(any(StartTransaction.class));
            verifyNoInteractions(transaction);

            start.complete(transaction);
            verify(transaction, timeout(2_000).times(2)).advanceConsumer(any(AdvanceConsumer.class));
            verify(transaction, never()).commit();

            firstAdvance.complete(null);
            verify(transaction, never()).commit();

            secondAdvance.complete(null);
            verify(transaction, timeout(2_000)).commit();
            verify(transaction, never()).close();
            verifyNoInteractions(commitsSucceeded);

            commit.complete(null);
            verify(transaction, timeout(2_000)).close();
        }
        verify(commitsSucceeded).inc();
        verifyNoInteractions(commitsFailed);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void keepsOnlyLatestCompletedCheckpointWhileCommitIsRunning(boolean firstCommitFails) {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction firstTransaction = successfulTransaction(client);
        ApiServiceTransaction latestTransaction = successfulTransaction(client);
        CompletableFuture<Void> firstCommit = new CompletableFuture<>();
        when(firstTransaction.commit()).thenReturn(firstCommit);
        when(client.startTransaction(any(StartTransaction.class)))
                .thenReturn(CompletableFuture.completedFuture(firstTransaction))
                .thenReturn(CompletableFuture.completedFuture(latestTransaction));
        try (YTsaurusQueueConsumerOffsetCommitter committer = committer(client)) {
            for (long checkpointId = 1; checkpointId <= 4; checkpointId++) {
                committer.snapshotState(checkpointId, List.of(split("queue-id", 0, checkpointId * 10)));
            }
            committer.notifyCheckpointComplete(1);
            verify(firstTransaction, timeout(2_000)).commit();
            committer.notifyCheckpointAborted(2);
            committer.notifyCheckpointComplete(3);
            committer.notifyCheckpointComplete(4);
            committer.notifyCheckpointComplete(3);
            verifyNoInteractions(latestTransaction);
            verify(client).startTransaction(any(StartTransaction.class));

            if (firstCommitFails) {
                firstCommit.completeExceptionally(new IOException("commit failed"));
            } else {
                firstCommit.complete(null);
            }
            verify(latestTransaction, timeout(2_000)).close();
        }

        ArgumentCaptor<AdvanceConsumer> advances = ArgumentCaptor.forClass(AdvanceConsumer.class);
        verify(firstTransaction).advanceConsumer(advances.capture());
        verify(latestTransaction).advanceConsumer(advances.capture());
        assertThat(advances.getAllValues()).extracting("newOffset").containsExactly(10L, 40L);
        verify(firstTransaction).close();
        verify(client, times(2)).startTransaction(any(StartTransaction.class));
        verify(commitsSucceeded, times(firstCommitFails ? 1 : 2)).inc();
        verify(commitsFailed, times(firstCommitFails ? 1 : 0)).inc();
    }

    @Test
    void abortedAndUnknownCheckpointsDoNotCommit() {
        ApiServiceClient client = mock(ApiServiceClient.class);
        try (YTsaurusQueueConsumerOffsetCommitter committer = committer(client)) {
            committer.snapshotState(1, List.of(split("queue-id", 0, 10)));
            committer.notifyCheckpointAborted(1);
            committer.notifyCheckpointComplete(1);
            committer.notifyCheckpointComplete(2);
        }
        verifyNoInteractions(client);
    }

    @Test
    void emptySnapshotCompletesWithoutTransaction() {
        ApiServiceClient client = mock(ApiServiceClient.class);
        try (YTsaurusQueueConsumerOffsetCommitter committer = committer(client)) {
            committer.snapshotState(1, List.of());
            committer.notifyCheckpointComplete(1);
        }
        verifyNoInteractions(client);
    }

    @Test
    void partialAdvancementFailureIsCountedWithoutCommittingTransaction() {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = successfulTransaction(client);
        when(transaction.advanceConsumer(any(AdvanceConsumer.class)))
                .thenReturn(CompletableFuture.completedFuture(null))
                .thenReturn(CompletableFuture.failedFuture(new IOException("advance failed")));
        try (YTsaurusQueueConsumerOffsetCommitter committer = committer(client)) {
            committer.snapshotState(1, List.of(split("queue-id", 0, 10), split("queue-id", 1, 20)));
            committer.notifyCheckpointComplete(1);
            verify(commitsFailed, timeout(2_000)).inc();
        }

        verify(transaction, never()).commit();
        verify(transaction, times(2)).advanceConsumer(any(AdvanceConsumer.class));
        verify(transaction).close();
        verifyNoInteractions(commitsSucceeded);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void startFailureDoesNotPreventLaterCommit(boolean synchronousFailure) {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = successfulTransaction(client);
        RuntimeException failure = new RuntimeException("start failed");
        if (synchronousFailure) {
            when(client.startTransaction(any(StartTransaction.class))).thenThrow(failure)
                    .thenReturn(CompletableFuture.completedFuture(transaction));
        } else {
            when(client.startTransaction(any(StartTransaction.class))).thenReturn(CompletableFuture.failedFuture(failure))
                    .thenReturn(CompletableFuture.completedFuture(transaction));
        }
        try (YTsaurusQueueConsumerOffsetCommitter committer = committer(client)) {
            committer.snapshotState(1, List.of(split("queue-id", 0, 10)));
            committer.notifyCheckpointComplete(1);
            verify(commitsFailed, timeout(2_000)).inc();
            verifyNoInteractions(transaction, commitsSucceeded);

            committer.snapshotState(2, List.of(split("queue-id", 0, 20)));
            committer.notifyCheckpointComplete(2);
            verify(transaction, timeout(2_000)).close();
        }

        verify(transaction).commit();
        verify(commitsSucceeded).inc();
        verify(commitsFailed).inc();
    }

    @Test
    void commitFailureDiscardsOlderSnapshotsAndPreservesLaterCheckpoint() {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction failedTransaction = successfulTransaction(client);
        ApiServiceTransaction laterTransaction = successfulTransaction(client);
        when(client.startTransaction(any(StartTransaction.class)))
                .thenReturn(CompletableFuture.completedFuture(failedTransaction))
                .thenReturn(CompletableFuture.completedFuture(laterTransaction));
        IOException failure = new IOException("commit failed");
        when(failedTransaction.commit()).thenReturn(CompletableFuture.failedFuture(failure));
        RuntimeException closeFailure = new RuntimeException("close failed");
        doThrow(closeFailure).when(failedTransaction).close();
        try (YTsaurusQueueConsumerOffsetCommitter committer = committer(client)) {
            committer.snapshotState(1, List.of(split("queue-id", 0, 10)));
            committer.snapshotState(2, List.of(split("queue-id", 0, 20)));
            committer.snapshotState(3, List.of(split("queue-id", 0, 30)));
            committer.notifyCheckpointComplete(2);
            verify(commitsFailed, timeout(2_000)).inc();
            verifyNoInteractions(commitsSucceeded);

            committer.notifyCheckpointComplete(1);
            committer.notifyCheckpointComplete(2);
            committer.notifyCheckpointAborted(2);
            verifyNoInteractions(laterTransaction);
            committer.notifyCheckpointComplete(3);
            verify(laterTransaction, timeout(2_000)).close();
        }

        ArgumentCaptor<AdvanceConsumer> advances = ArgumentCaptor.forClass(AdvanceConsumer.class);
        verify(failedTransaction).advanceConsumer(advances.capture());
        verify(laterTransaction).advanceConsumer(advances.capture());
        assertAdvance(advances.getAllValues().get(0), 0, 20);
        assertAdvance(advances.getAllValues().get(1), 0, 30);
        verify(client, times(2)).startTransaction(any(StartTransaction.class));
        verify(laterTransaction).commit();
        assertThat(failure.getSuppressed()).containsExactly(closeFailure);
        verify(commitsSucceeded).inc();
        verify(commitsFailed).inc();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void propagatesErrorsFromCommitOrClose(boolean failOnClose) {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = successfulTransaction(client);
        AssertionError error = new AssertionError("transaction failed");
        if (failOnClose) {
            when(transaction.commit()).thenReturn(CompletableFuture.failedFuture(new IOException("commit failed")));
            doThrow(error).when(transaction).close();
        } else {
            when(transaction.commit()).thenReturn(CompletableFuture.failedFuture(error));
        }
        try (YTsaurusQueueConsumerOffsetCommitter committer = committer(client)) {
            committer.snapshotState(1, List.of(split("queue-id", 0, 10)));
            committer.notifyCheckpointComplete(1);
            assertThatThrownBy(() -> committer.getFailureFuture().get(2, TimeUnit.SECONDS)).hasCause(error);
        }
        verifyNoInteractions(commitsFailed);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void fatalFailureStopsWaitingCommits(boolean interrupted) {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = successfulTransaction(client);
        CompletableFuture<Void> advance = new CompletableFuture<>();
        when(transaction.advanceConsumer(any(AdvanceConsumer.class))).thenReturn(advance);
        Throwable failure;
        if (interrupted) {
            failure = new InterruptedException("advance interrupted");
        } else {
            failure = new AssertionError("advance failed");
        }
        try (YTsaurusQueueConsumerOffsetCommitter committer = committer(client)) {
            committer.snapshotState(1, List.of(split("queue-id", 0, 10)));
            committer.notifyCheckpointComplete(1);
            verify(transaction, timeout(2_000)).advanceConsumer(any(AdvanceConsumer.class));
            committer.snapshotState(2, List.of(split("queue-id", 0, 20)));
            committer.notifyCheckpointComplete(2);
            advance.completeExceptionally(failure);
            assertThatThrownBy(() -> committer.getFailureFuture().get(2, TimeUnit.SECONDS)).hasCause(failure);
        }
        verify(client).startTransaction(any(StartTransaction.class));
        verify(transaction).close();
        verify(transaction, never()).commit();
        verifyNoInteractions(commitsFailed);
    }

    @Test
    void closeWaitsForActiveCommitAndDiscardsWaitingCheckpoint() throws Exception {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = successfulTransaction(client);
        CompletableFuture<Void> commit = new CompletableFuture<>();
        when(transaction.commit()).thenReturn(commit);
        ExecutorService closer = Executors.newSingleThreadExecutor();
        CountDownLatch closeStarted = new CountDownLatch(1);
        try (YTsaurusQueueConsumerOffsetCommitter committer = committer(client)) {
            committer.snapshotState(1, List.of(split("queue-id", 0, 10)));
            committer.notifyCheckpointComplete(1);
            verify(transaction, timeout(2_000)).commit();
            committer.snapshotState(2, List.of(split("queue-id", 0, 20)));
            committer.notifyCheckpointComplete(2);
            Future<?> close = closer.submit(() -> {
                closeStarted.countDown();
                committer.close();
            });
            assertThat(closeStarted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> close.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);

            commit.complete(null);
            close.get(2, TimeUnit.SECONDS);
            verify(transaction).close();
            verify(client).startTransaction(any(StartTransaction.class));
        } finally {
            commit.complete(null);
            closer.shutdownNow();
        }
    }

    @Test
    void closeDoesNotWaitForPendingTransactionStart() throws Exception {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = mock(ApiServiceTransaction.class);
        CompletableFuture<ApiServiceTransaction> start = new CompletableFuture<>();
        CompletableFuture<Thread> worker = new CompletableFuture<>();
        when(client.startTransaction(any(StartTransaction.class))).thenAnswer(ignored -> {
            worker.complete(Thread.currentThread());
            return start;
        });
        try (YTsaurusQueueConsumerOffsetCommitter committer = new YTsaurusQueueConsumerOffsetCommitter(
                client, CONSUMER_PATH, QUEUE_PATH, commitsSucceeded, commitsFailed, 0)) {
            committer.snapshotState(1, List.of(split("queue-id", 0, 10)));
            committer.notifyCheckpointComplete(1);
            Thread workerThread = worker.get(2, TimeUnit.SECONDS);

            committer.close();
            workerThread.join(2_000);
            assertThat(workerThread.isAlive()).isFalse();
            start.complete(transaction);
            assertThat(start).isNotCancelled();
            assertThat(committer.getFailureFuture()).isNotDone();
        }
        verifyNoInteractions(transaction, commitsFailed);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closeTimeoutInterruptsWorkerAndClosesStartedTransaction(boolean duringCommit) {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = successfulTransaction(client);
        CompletableFuture<Void> rpc = new CompletableFuture<>();
        if (duringCommit) {
            when(transaction.commit()).thenReturn(rpc);
        } else {
            when(transaction.advanceConsumer(any(AdvanceConsumer.class))).thenReturn(rpc);
        }
        try (YTsaurusQueueConsumerOffsetCommitter committer = new YTsaurusQueueConsumerOffsetCommitter(
                client, CONSUMER_PATH, QUEUE_PATH, commitsSucceeded, commitsFailed, 0)) {
            committer.snapshotState(1, List.of(split("queue-id", 0, 10)));
            committer.notifyCheckpointComplete(1);
            if (duringCommit) {
                verify(transaction, timeout(2_000)).commit();
            } else {
                verify(transaction, timeout(2_000)).advanceConsumer(any(AdvanceConsumer.class));
            }
            committer.snapshotState(2, List.of(split("queue-id", 0, 20)));
            committer.notifyCheckpointComplete(2);
            committer.close();
            committer.notifyCheckpointComplete(2);
            verify(transaction, timeout(2_000)).close();
            assertThat(committer.getFailureFuture()).isNotDone();
        }

        verify(client).startTransaction(any(StartTransaction.class));
        assertThat(rpc).isNotCancelled();
        if (!duringCommit) {
            verify(transaction, never()).commit();
        }
        verifyNoInteractions(commitsFailed);
    }

    @Test
    void interruptedClosePreservesInterruptFlag() {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = successfulTransaction(client);
        when(transaction.commit()).thenReturn(new CompletableFuture<>());
        try (YTsaurusQueueConsumerOffsetCommitter committer = committer(client)) {
            committer.snapshotState(1, List.of(split("queue-id", 0, 10)));
            committer.notifyCheckpointComplete(1);
            verify(transaction, timeout(2_000)).commit();

            Thread.currentThread().interrupt();
            try {
                committer.close();
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
            } finally {
                Thread.interrupted();
            }
            verify(transaction, timeout(2_000)).close();
            assertThat(committer.getFailureFuture()).isNotDone();
        }
        verifyNoInteractions(commitsFailed);
    }

    @Test
    void shutdownInterruptionDoesNotHideCloseError() {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = successfulTransaction(client);
        when(transaction.commit()).thenReturn(new CompletableFuture<>());
        AssertionError error = new AssertionError("close failed");
        doThrow(error).when(transaction).close();
        try (YTsaurusQueueConsumerOffsetCommitter committer = new YTsaurusQueueConsumerOffsetCommitter(
                client, CONSUMER_PATH, QUEUE_PATH, commitsSucceeded, commitsFailed, 0)) {
            committer.snapshotState(1, List.of(split("queue-id", 0, 10)));
            committer.notifyCheckpointComplete(1);
            verify(transaction, timeout(2_000)).commit();

            committer.close();
            assertThatThrownBy(() -> committer.getFailureFuture().get(2, TimeUnit.SECONDS)).hasCause(error);
        }
        verifyNoInteractions(commitsFailed);
    }

    @Test
    void closeDiscardsSnapshotsAndIgnoresLateNotifications() {
        ApiServiceClient client = mock(ApiServiceClient.class);
        try (YTsaurusQueueConsumerOffsetCommitter committer = committer(client)) {
            committer.snapshotState(1, List.of(split("queue-id", 0, 10)));
            committer.close();
            committer.notifyCheckpointComplete(1);
            committer.notifyCheckpointAborted(1);
            assertThatThrownBy(() -> committer.snapshotState(2, List.of()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Queue consumer offset committer is closed");
        }
        verifyNoInteractions(client);
    }

    private static ApiServiceTransaction successfulTransaction(ApiServiceClient client) {
        ApiServiceTransaction transaction = mock(ApiServiceTransaction.class);
        when(client.startTransaction(any(StartTransaction.class))).thenReturn(
                CompletableFuture.completedFuture(transaction));
        when(transaction.advanceConsumer(any(AdvanceConsumer.class))).thenReturn(
                CompletableFuture.completedFuture(null));
        when(transaction.commit()).thenReturn(CompletableFuture.completedFuture(null));
        return transaction;
    }

    private YTsaurusQueueConsumerOffsetCommitter committer(ApiServiceClient client) {
        return new YTsaurusQueueConsumerOffsetCommitter(
                client, CONSUMER_PATH, QUEUE_PATH, commitsSucceeded, commitsFailed, 5_000);
    }

    private static void assertAdvance(
            AdvanceConsumer request,
            int partitionIndex,
            long newOffset) {
        assertThat(request).extracting(
                        "consumerPath",
                        "queuePath",
                        "partitionIndex",
                        "oldOffset",
                        "newOffset")
                .containsExactly(
                        YPath.simple(CONSUMER_PATH),
                        YPath.simple(QUEUE_PATH),
                        partitionIndex,
                        null,
                        newOffset);
    }
}
