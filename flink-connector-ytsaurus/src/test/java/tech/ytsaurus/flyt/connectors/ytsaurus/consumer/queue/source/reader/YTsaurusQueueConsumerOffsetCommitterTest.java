package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.junit.jupiter.api.Test;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueTestFixtures.split;

class YTsaurusQueueConsumerOffsetCommitterTest {
    private static final String CONSUMER_PATH = "//home/test/consumer";

    private static final String QUEUE_PATH = "//home/test/queue";

    private final Counter commitsFailed = new SimpleCounter();

    @Test
    void commitsSnapshotOffsetsOnlyAfterCheckpointCompletion() throws Exception {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = successfulTransaction(client);
        YTsaurusQueueConsumerOffsetCommitter committer = committer(client);
        List<YtQueueSplit> offsets = new ArrayList<>(List.of(
                split("queue-id", 2, 23),
                split("queue-id", 0, 17)));

        committer.snapshotState(11, offsets);
        offsets.clear();

        verifyNoInteractions(client, transaction);

        committer.notifyCheckpointComplete(11);

        ArgumentCaptor<StartTransaction> startRequest = ArgumentCaptor.forClass(StartTransaction.class);
        verify(client).startTransaction(startRequest.capture());
        assertThat(startRequest.getValue().getType()).isEqualTo(TransactionType.Tablet);
        ArgumentCaptor<AdvanceConsumer> advances = ArgumentCaptor.forClass(AdvanceConsumer.class);
        verify(transaction, times(2)).advanceConsumer(advances.capture());
        assertAdvance(advances.getAllValues().get(0), 0, 17);
        assertAdvance(advances.getAllValues().get(1), 2, 23);
        verify(transaction).commit();
        verify(transaction).close();
    }

    @Test
    void laterCompletedCheckpointSubsumesOlderSnapshots() throws Exception {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = successfulTransaction(client);
        YTsaurusQueueConsumerOffsetCommitter committer = committer(client);
        committer.snapshotState(1, List.of(split("queue-id", 0, 10)));
        committer.snapshotState(2, List.of(split("queue-id", 0, 20)));

        committer.notifyCheckpointComplete(2);
        committer.notifyCheckpointComplete(1);
        committer.notifyCheckpointAborted(1);
        committer.notifyCheckpointComplete(2);

        ArgumentCaptor<AdvanceConsumer> request = ArgumentCaptor.forClass(AdvanceConsumer.class);
        verify(transaction).advanceConsumer(request.capture());
        assertAdvance(request.getValue(), 0, 20);
        verify(client).startTransaction(any(StartTransaction.class));
    }

    @Test
    void abortedAndUnknownCheckpointsDoNotCommit() throws Exception {
        ApiServiceClient client = mock(ApiServiceClient.class);
        YTsaurusQueueConsumerOffsetCommitter committer = committer(client);
        committer.snapshotState(1, List.of(split("queue-id", 0, 10)));

        committer.notifyCheckpointAborted(1);
        committer.notifyCheckpointComplete(1);
        committer.notifyCheckpointComplete(2);

        verifyNoInteractions(client);
    }

    @Test
    void emptySnapshotCompletesWithoutTransaction() throws Exception {
        ApiServiceClient client = mock(ApiServiceClient.class);
        YTsaurusQueueConsumerOffsetCommitter committer = committer(client);
        committer.snapshotState(1, List.of());

        committer.notifyCheckpointComplete(1);

        verifyNoInteractions(client);
    }

    @Test
    void partialAdvancementFailureIsCountedWithoutCommittingTransaction() throws Exception {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = mock(ApiServiceTransaction.class);
        when(client.startTransaction(any(StartTransaction.class))).thenReturn(
                CompletableFuture.completedFuture(transaction));
        when(transaction.advanceConsumer(any(AdvanceConsumer.class)))
                .thenReturn(CompletableFuture.completedFuture(null))
                .thenReturn(CompletableFuture.failedFuture(new IOException("advance failed")));
        YTsaurusQueueConsumerOffsetCommitter committer = committer(client);
        committer.snapshotState(1, List.of(split("queue-id", 0, 10), split("queue-id", 1, 20)));

        committer.notifyCheckpointComplete(1);

        assertThat(commitsFailed.getCount()).isEqualTo(1);
        verify(transaction, never()).commit();
        verify(transaction, times(2)).advanceConsumer(any(AdvanceConsumer.class));
        verify(transaction).close();
    }

    @Test
    void commitFailureDiscardsOlderSnapshotsAndPreservesLaterCheckpoint() throws Exception {
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
        YTsaurusQueueConsumerOffsetCommitter committer = committer(client);
        committer.snapshotState(1, List.of(split("queue-id", 0, 10)));
        committer.snapshotState(2, List.of(split("queue-id", 0, 20)));
        committer.snapshotState(3, List.of(split("queue-id", 0, 30)));

        committer.notifyCheckpointComplete(2);

        assertThat(commitsFailed.getCount()).isEqualTo(1);
        assertThat(failure).hasSuppressedException(closeFailure);
        verify(failedTransaction).commit();
        verify(failedTransaction).close();

        committer.notifyCheckpointComplete(1);
        committer.notifyCheckpointComplete(2);
        committer.notifyCheckpointAborted(2);
        verifyNoInteractions(laterTransaction);

        committer.notifyCheckpointComplete(3);

        ArgumentCaptor<AdvanceConsumer> advances = ArgumentCaptor.forClass(AdvanceConsumer.class);
        verify(failedTransaction).advanceConsumer(advances.capture());
        verify(laterTransaction).advanceConsumer(advances.capture());
        assertAdvance(advances.getAllValues().get(0), 0, 20);
        assertAdvance(advances.getAllValues().get(1), 0, 30);
        verify(client, times(2)).startTransaction(any(StartTransaction.class));
        verify(laterTransaction).commit();
        verify(laterTransaction).close();
        assertThat(commitsFailed.getCount()).isEqualTo(1);
    }

    @Test
    void propagatesInterruptionAndPreservesInterruptFlag() throws Exception {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = successfulTransaction(client);
        when(transaction.commit()).thenReturn(new CompletableFuture<>());
        YTsaurusQueueConsumerOffsetCommitter committer = committer(client);
        committer.snapshotState(1, List.of(split("queue-id", 0, 10)));

        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> committer.notifyCheckpointComplete(1))
                    .isInstanceOf(InterruptedException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }

        verify(transaction).close();
        assertThat(commitsFailed.getCount()).isZero();
        committer.notifyCheckpointComplete(1);
        verify(client).startTransaction(any(StartTransaction.class));
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
        YTsaurusQueueConsumerOffsetCommitter committer = committer(client);
        committer.snapshotState(1, List.of(split("queue-id", 0, 10)));

        assertThatThrownBy(() -> committer.notifyCheckpointComplete(1)).isSameAs(error);

        verify(transaction).close();
        assertThat(commitsFailed.getCount()).isZero();
    }

    @Test
    void closeDiscardsSnapshotsAndIgnoresLateNotifications() throws Exception {
        ApiServiceClient client = mock(ApiServiceClient.class);
        YTsaurusQueueConsumerOffsetCommitter committer = committer(client);
        committer.snapshotState(1, List.of(split("queue-id", 0, 10)));

        committer.close();
        committer.close();
        committer.notifyCheckpointComplete(1);
        committer.notifyCheckpointAborted(1);

        assertThatThrownBy(() -> committer.snapshotState(2, List.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Queue consumer offset committer is closed");
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
                client,
                CONSUMER_PATH,
                QUEUE_PATH,
                commitsFailed);
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
