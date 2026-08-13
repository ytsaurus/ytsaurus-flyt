package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tech.ytsaurus.client.ApiServiceClient;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.PullQueue;
import tech.ytsaurus.client.rows.QueueRowset;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueueBatch;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueuePullRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class DirectYtQueuePullerTest {
    private static final String QUEUE_PATH = "//home/test/queue";

    @Test
    void pullUsesConfiguredPathAndAdaptsAllParametersAndResult() throws Exception {
        ApiServiceClient client = mock(ApiServiceClient.class);
        TableSchema schema = schema();
        UnversionedRow row = row();
        when(client.pullQueue(any())).thenReturn(
                CompletableFuture.completedFuture(rowset(schema, 42, List.of(row))));
        DirectYtQueuePuller puller = new DirectYtQueuePuller(client, QUEUE_PATH);

        YtQueueBatch batch = puller.pull(request(3, 42)).join();

        assertThat(batch.getSchema()).isSameAs(schema);
        assertThat(batch.getStartOffset()).isEqualTo(42);
        assertThat(batch.getFinishOffset()).isEqualTo(43);
        assertThat(batch.getRows()).containsExactly(row);
        ArgumentCaptor<PullQueue> request = ArgumentCaptor.forClass(PullQueue.class);
        verify(client).pullQueue(request.capture());
        assertThat(request.getValue().getArgumentsLogString())
                .contains("queuePath: //home/test/queue")
                .contains("partitionIndex: 3")
                .contains("offset: 42")
                .contains("maxRowCount=5")
                .contains("maxDataWeight=64");
        verifyNoMoreInteractions(client);

        puller.close();
        verifyNoMoreInteractions(client);
    }

    @Test
    void concurrentPullIsRejectedUntilFirstRpcCompletes() {
        ApiServiceClient client = mock(ApiServiceClient.class);
        TableSchema schema = mock(TableSchema.class);
        CompletableFuture<QueueRowset> firstRpc = new CompletableFuture<>();
        when(client.pullQueue(any()))
                .thenReturn(firstRpc)
                .thenReturn(CompletableFuture.completedFuture(rowset(schema, 0, List.of())));
        DirectYtQueuePuller puller = new DirectYtQueuePuller(client, QUEUE_PATH);
        YtQueuePullRequest request = request(0, 0);

        CompletableFuture<YtQueueBatch> firstPull = puller.pull(request);
        CompletableFuture<YtQueueBatch> concurrentPull = puller.pull(request);

        assertThatThrownBy(concurrentPull::join)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("Concurrent queue pulls are not supported");
        verify(client).pullQueue(any());

        firstRpc.complete(rowset(schema, 0, List.of()));
        firstPull.join();
        puller.pull(request).join();
        verify(client, times(2)).pullQueue(any());
    }

    @Test
    void synchronousClientFailureClearsInFlightPull() {
        ApiServiceClient client = mock(ApiServiceClient.class);
        TableSchema schema = mock(TableSchema.class);
        when(client.pullQueue(any()))
                .thenThrow(new IllegalStateException("rpc setup failed"))
                .thenReturn(CompletableFuture.completedFuture(rowset(schema, 0, List.of())));
        DirectYtQueuePuller puller = new DirectYtQueuePuller(client, QUEUE_PATH);
        YtQueuePullRequest request = request(0, 0);

        CompletableFuture<YtQueueBatch> failedPull = puller.pull(request);

        assertThatThrownBy(failedPull::join)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("rpc setup failed");
        puller.pull(request).join();
        verify(client, times(2)).pullQueue(any());
    }

    @Test
    void wakeUpCancelsInFlightRpcAndAllowsNextPull() {
        ApiServiceClient client = mock(ApiServiceClient.class);
        TableSchema schema = mock(TableSchema.class);
        CompletableFuture<QueueRowset> rpcFuture = new CompletableFuture<>();
        when(client.pullQueue(any()))
                .thenReturn(rpcFuture)
                .thenReturn(CompletableFuture.completedFuture(rowset(schema, 0, List.of())));
        DirectYtQueuePuller puller = new DirectYtQueuePuller(client, QUEUE_PATH);
        YtQueuePullRequest request = request(0, 0);

        puller.pull(request);
        puller.wakeUp();

        assertThat(rpcFuture).isCancelled();
        puller.pull(request).join();
        verify(client, times(2)).pullQueue(any());
    }

    @Test
    void wakeUpCannotMissRpcBeforeItIsPublished() throws Exception {
        ApiServiceClient client = mock(ApiServiceClient.class);
        CompletableFuture<QueueRowset> rpcFuture = new CompletableFuture<>();
        CountDownLatch rpcStarted = new CountDownLatch(1);
        CountDownLatch allowRpcReturn = new CountDownLatch(1);
        CountDownLatch wakeUpStarted = new CountDownLatch(1);
        when(client.pullQueue(any())).thenAnswer(invocation -> {
            rpcStarted.countDown();
            assertThat(allowRpcReturn.await(5, TimeUnit.SECONDS)).isTrue();
            return rpcFuture;
        });
        DirectYtQueuePuller puller = new DirectYtQueuePuller(client, QUEUE_PATH);

        CompletableFuture<Void> pullCall = CompletableFuture.runAsync(() ->
                puller.pull(request(0, 0)));
        assertThat(rpcStarted.await(5, TimeUnit.SECONDS)).isTrue();
        CompletableFuture<Void> wakeUpCall = CompletableFuture.runAsync(() -> {
            wakeUpStarted.countDown();
            puller.wakeUp();
        });
        assertThat(wakeUpStarted.await(5, TimeUnit.SECONDS)).isTrue();
        allowRpcReturn.countDown();

        pullCall.get(5, TimeUnit.SECONDS);
        wakeUpCall.get(5, TimeUnit.SECONDS);
        assertThat(rpcFuture).isCancelled();
    }

    @Test
    void closeCancelsInFlightRpcAndClosesOwnedClientOnce() throws Exception {
        YTsaurusClient client = mock(YTsaurusClient.class);
        CompletableFuture<QueueRowset> rpcFuture = new CompletableFuture<>();
        when(client.pullQueue(any())).thenReturn(rpcFuture);
        DirectYtQueuePuller puller = new DirectYtQueuePuller(client, QUEUE_PATH);

        puller.pull(request(0, 0));
        puller.close();
        puller.close();

        assertThat(rpcFuture).isCancelled();
        verify(client).close();
    }

    @Test
    void closeCannotMissRpcBeforeItIsPublished() throws Exception {
        YTsaurusClient client = mock(YTsaurusClient.class);
        CompletableFuture<QueueRowset> rpcFuture = new CompletableFuture<>();
        CountDownLatch rpcStarted = new CountDownLatch(1);
        CountDownLatch allowRpcReturn = new CountDownLatch(1);
        CountDownLatch closeStarted = new CountDownLatch(1);
        when(client.pullQueue(any())).thenAnswer(invocation -> {
            rpcStarted.countDown();
            assertThat(allowRpcReturn.await(5, TimeUnit.SECONDS)).isTrue();
            return rpcFuture;
        });
        DirectYtQueuePuller puller = new DirectYtQueuePuller(client, QUEUE_PATH);

        CompletableFuture<Void> pullCall = CompletableFuture.runAsync(() ->
                puller.pull(request(0, 0)));
        assertThat(rpcStarted.await(5, TimeUnit.SECONDS)).isTrue();
        CompletableFuture<Void> closeCall = CompletableFuture.runAsync(() -> {
            closeStarted.countDown();
            close(puller);
        });
        assertThat(closeStarted.await(5, TimeUnit.SECONDS)).isTrue();
        allowRpcReturn.countDown();

        pullCall.get(5, TimeUnit.SECONDS);
        closeCall.get(5, TimeUnit.SECONDS);
        assertThat(rpcFuture).isCancelled();
        verify(client).close();
    }

    @Test
    void closedPullerRejectsPullWithoutCallingClient() throws Exception {
        ApiServiceClient client = mock(ApiServiceClient.class);
        DirectYtQueuePuller puller = new DirectYtQueuePuller(client, QUEUE_PATH);
        puller.close();

        CompletableFuture<YtQueueBatch> result = puller.pull(
                request(0, 0));

        assertThatThrownBy(result::join)
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("Queue puller is closed");
        verifyNoInteractions(client);
    }

    private static void close(DirectYtQueuePuller puller) {
        try {
            puller.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static YtQueuePullRequest request(int partitionIndex, long offset) {
        return new YtQueuePullRequest(partitionIndex, offset, 5, 64);
    }

    private static QueueRowset rowset(
            TableSchema schema,
            long startOffset,
            List<UnversionedRow> rows) {
        return new QueueRowset(
                new tech.ytsaurus.client.rows.UnversionedRowset(schema, rows),
                startOffset);
    }

    private static TableSchema schema() {
        return TableSchema.builder().build();
    }

    private static UnversionedRow row() {
        return new UnversionedRow(List.of());
    }
}
