package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tech.ytsaurus.client.ApiServiceClient;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.PullConsumer;
import tech.ytsaurus.client.rows.QueueRowset;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.core.tables.TableSchema;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueueBatch;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueuePullRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class ConsumerYtQueuePullerTest {
    private static final String CONSUMER_PATH = "//home/test/consumer";

    private static final String QUEUE_PATH = "//home/test/queue";

    @Test
    void pullUsesConsumerAndQueuePathsAndExplicitSplitOffset() throws Exception {
        ApiServiceClient client = mock(ApiServiceClient.class);
        TableSchema schema = TableSchema.builder().build();
        UnversionedRow row = new UnversionedRow(List.of());
        when(client.pullConsumer(any())).thenReturn(CompletableFuture.completedFuture(
                new QueueRowset(new UnversionedRowset(schema, List.of(row)), 42)));
        ConsumerYtQueuePuller puller = new ConsumerYtQueuePuller(
                client,
                CONSUMER_PATH,
                QUEUE_PATH);

        YtQueueBatch batch = puller.pull(new YtQueuePullRequest(3, 42, 5, 64)).join();

        assertThat(batch.getStartOffset()).isEqualTo(42);
        assertThat(batch.getRows()).containsExactly(row);
        ArgumentCaptor<PullConsumer> request = ArgumentCaptor.forClass(PullConsumer.class);
        verify(client).pullConsumer(request.capture());
        assertThat(request.getValue().getArgumentsLogString())
                .contains("consumerPath: //home/test/consumer")
                .contains("queuePath: //home/test/queue")
                .contains("partitionIndex: 3")
                .contains("offset: 42")
                .contains("maxRowCount=5")
                .contains("maxDataWeight=64");

        puller.close();
        verifyNoMoreInteractions(client);
    }

    @Test
    void owningPullerClosesClient() throws Exception {
        YTsaurusClient client = mock(YTsaurusClient.class);
        ConsumerYtQueuePuller puller = new ConsumerYtQueuePuller(
                client,
                CONSUMER_PATH,
                QUEUE_PATH);

        puller.close();
        puller.close();

        verify(client).close();
    }
}
