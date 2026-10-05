package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
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

    @Test
    void commitsSnapshotOffsetsOnlyAfterCheckpointCompletion() throws Exception {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = successfulTransaction(client);
        YTsaurusQueueConsumerOffsetCommitter committer = committer(client);
        List<YtQueueSplit> offsets = List.of(
                split("queue-id", 2, 23),
                split("queue-id", 0, 17));

        committer.snapshotState(11, offsets);

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
    void advancementFailureFailsCheckpointAndDoesNotCommitTransaction() {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction transaction = mock(ApiServiceTransaction.class);
        when(client.startTransaction(any(StartTransaction.class))).thenReturn(
                CompletableFuture.completedFuture(transaction));
        when(transaction.advanceConsumer(any(AdvanceConsumer.class))).thenReturn(
                CompletableFuture.failedFuture(new IOException("advance failed")));
        YTsaurusQueueConsumerOffsetCommitter committer = committer(client);
        committer.snapshotState(1, List.of(split("queue-id", 0, 10)));

        assertThatThrownBy(() -> committer.notifyCheckpointComplete(1))
                .isInstanceOf(IOException.class)
                .hasMessage("advance failed");

        verify(transaction, never()).commit();
        verify(transaction).close();
    }

    @Test
    void commitFailurePreservesSnapshotForRepeatedCheckpointCompletion() throws Exception {
        ApiServiceClient client = mock(ApiServiceClient.class);
        ApiServiceTransaction failedTransaction = successfulTransaction(client);
        ApiServiceTransaction retryTransaction = successfulTransaction(client);
        when(client.startTransaction(any(StartTransaction.class)))
                .thenReturn(CompletableFuture.completedFuture(failedTransaction))
                .thenReturn(CompletableFuture.completedFuture(retryTransaction));
        IOException failure = new IOException("commit failed");
        when(failedTransaction.commit()).thenReturn(CompletableFuture.failedFuture(failure));
        YTsaurusQueueConsumerOffsetCommitter committer = committer(client);
        committer.snapshotState(1, List.of(split("queue-id", 0, 10)));

        assertThatThrownBy(() -> committer.notifyCheckpointComplete(1)).isSameAs(failure);
        verify(failedTransaction).commit();
        verify(failedTransaction).close();
        verifyNoInteractions(retryTransaction);

        committer.notifyCheckpointComplete(1);
        committer.notifyCheckpointComplete(1);

        ArgumentCaptor<AdvanceConsumer> advances = ArgumentCaptor.forClass(AdvanceConsumer.class);
        verify(failedTransaction).advanceConsumer(advances.capture());
        verify(retryTransaction).advanceConsumer(advances.capture());
        assertThat(advances.getAllValues()).allSatisfy(request -> assertAdvance(request, 0, 10));
        verify(client, times(2)).startTransaction(any(StartTransaction.class));
        verify(retryTransaction).commit();
        verify(retryTransaction).close();
    }

    @Test
    void closeIsIdempotentAndRejectsFurtherCallbacks() {
        ApiServiceClient client = mock(ApiServiceClient.class);
        YTsaurusQueueConsumerOffsetCommitter committer = committer(client);

        committer.close();
        committer.close();

        assertThatThrownBy(() -> committer.snapshotState(1, List.of()))
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

    private static YTsaurusQueueConsumerOffsetCommitter committer(ApiServiceClient client) {
        return new YTsaurusQueueConsumerOffsetCommitter(
                client,
                CONSUMER_PATH,
                QUEUE_PATH);
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
