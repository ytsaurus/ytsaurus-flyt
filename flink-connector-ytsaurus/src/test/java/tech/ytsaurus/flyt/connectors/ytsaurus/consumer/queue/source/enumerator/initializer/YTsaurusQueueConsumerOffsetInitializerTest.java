package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.initializer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import tech.ytsaurus.client.DefaultSerializationResolver;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.client.request.LookupRowsRequest;
import tech.ytsaurus.client.request.PullConsumer;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueTestFixtures.metadata;

class YTsaurusQueueConsumerOffsetInitializerTest {
    private static final String CONSUMER_PATH = "//home/test/consumer";

    private static final String QUEUE_PATH = "//home/test/queue";

    private static final String QUEUE_CLUSTER = "cluster-from-attribute";

    @Test
    void readsStoredOffsetsWithoutTrimNormalizationAndCachesClusterName() {
        YTsaurusClient client = clientWithClusterName();
        UnversionedRowset firstRows = rowset(offsetRow(10L), offsetRow(23L));
        UnversionedRowset laterRows = rowset(offsetRow(17L));
        when(client.lookupRows(any(LookupRowsRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(firstRows))
                .thenReturn(CompletableFuture.completedFuture(laterRows));
        YTsaurusQueueConsumerOffsetInitializer initializer = initializer(client);

        Map<Integer, Long> offsets = initializer.getInitialOffsets(
                metadata("queue-id", 3),
                List.of(0, 2));
        Map<Integer, Long> laterOffsets = initializer.getInitialOffsets(
                metadata("queue-id", 3),
                List.of(1));
        initializer.close();

        assertThat(offsets).containsExactly(entry(0, 10L), entry(2, 23L));
        assertThat(laterOffsets).containsExactly(entry(1, 17L));
        ArgumentCaptor<GetNode> clusterRequest = ArgumentCaptor.forClass(GetNode.class);
        verify(client).getNode(clusterRequest.capture());
        assertThat(clusterRequest.getValue().getPath().toString()).isEqualTo("//sys/@cluster_name");
        ArgumentCaptor<LookupRowsRequest> requests = ArgumentCaptor.forClass(LookupRowsRequest.class);
        verify(client, times(2)).lookupRows(requests.capture());
        assertLookupRequest(requests.getAllValues().get(0), List.of(0L, 2L));
        assertLookupRequest(requests.getAllValues().get(1), List.of(1L));
        verify(client, never()).pullConsumer(any(PullConsumer.class));
        verify(client).close();
        verifyNoMoreInteractions(client);
    }

    @Test
    void missingConsumerRowStartsAtZeroWithoutShiftingOtherPartitions() {
        YTsaurusClient client = clientWithClusterName();
        UnversionedRowset rows = rowset(null, offsetRow(23L));
        when(client.lookupRows(any(LookupRowsRequest.class))).thenReturn(
                CompletableFuture.completedFuture(rows));
        YTsaurusQueueConsumerOffsetInitializer initializer = initializer(client);

        assertThat(initializer.getInitialOffsets(metadata("queue-id", 3), List.of(0, 2)))
                .containsExactly(entry(0, 0L), entry(2, 23L));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void propagatesRpcFailures(boolean failClusterLookup) {
        YTsaurusClient client = clientWithClusterName();
        IllegalStateException failure = new IllegalStateException("consumer unavailable");
        if (failClusterLookup) {
            when(client.getNode(any(GetNode.class))).thenReturn(CompletableFuture.failedFuture(failure));
        } else {
            when(client.lookupRows(any(LookupRowsRequest.class)))
                    .thenReturn(CompletableFuture.failedFuture(failure));
        }

        assertThatThrownBy(() -> initializer(client).getInitialOffsets(
                metadata("queue-id", 1),
                List.of(0)))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasRootCauseMessage("consumer unavailable");
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(longs = {-1})
    void rejectsNullAndNegativeStoredOffsets(Long offset) {
        YTsaurusClient client = clientWithClusterName();
        UnversionedRowset rows = rowset(offsetRow(offset));
        when(client.lookupRows(any(LookupRowsRequest.class))).thenReturn(
                CompletableFuture.completedFuture(rows));

        assertThatThrownBy(() -> initializer(client).getInitialOffsets(
                metadata("queue-id", 1),
                List.of(0)))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void validatesPartitionsBeforeCallingClient() {
        YTsaurusClient client = mock(YTsaurusClient.class);
        YTsaurusQueueConsumerOffsetInitializer initializer = initializer(client);

        assertThatThrownBy(() -> initializer.getInitialOffsets(
                metadata("queue-id", 1),
                List.of(1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partitionIndex");
        assertThat(initializer.getInitialOffsets(metadata("queue-id", 1), List.of())).isEmpty();
        verifyNoInteractions(client);
    }

    private static YTsaurusQueueConsumerOffsetInitializer initializer(YTsaurusClient client) {
        return new YTsaurusQueueConsumerOffsetInitializer(
                client,
                CONSUMER_PATH,
                QUEUE_PATH);
    }

    private static YTsaurusClient clientWithClusterName() {
        YTsaurusClient client = mock(YTsaurusClient.class);
        when(client.getNode(any(GetNode.class))).thenReturn(
                CompletableFuture.completedFuture(YTree.stringNode(QUEUE_CLUSTER)));
        return client;
    }

    private static UnversionedRowset rowset(YTreeMapNode... rows) {
        UnversionedRowset rowset = mock(UnversionedRowset.class);
        when(rowset.getYTreeRows()).thenReturn(Arrays.asList(rows));
        return rowset;
    }

    private static YTreeMapNode offsetRow(Long offset) {
        return YTree.mapBuilder().key("offset").value((Object) offset).buildMap();
    }

    private static void assertLookupRequest(LookupRowsRequest request, List<Long> partitions) {
        assertThat(request.getPath()).isEqualTo(CONSUMER_PATH);
        assertThat(request.getLookupColumns()).containsExactly("offset");
        assertThat(request.getKeepMissingRows()).isTrue();
        assertThat(request.getSchema().getColumns())
                .extracting(ColumnSchema::getName, ColumnSchema::getType)
                .containsExactly(
                        tuple("queue_cluster", ColumnValueType.STRING),
                        tuple("queue_path", ColumnValueType.STRING),
                        tuple("partition_index", ColumnValueType.UINT64));
        request.convertValues(DefaultSerializationResolver.getInstance());
        assertThat(request).extracting("filters", list(UnversionedRow.class))
                .allSatisfy(row -> {
                    assertThat(row.getValues().get(0).stringValue()).isEqualTo(QUEUE_CLUSTER);
                    assertThat(row.getValues().get(1).stringValue()).isEqualTo(QUEUE_PATH);
                    assertThat(row.getValues().get(2).getType()).isEqualTo(ColumnValueType.UINT64);
                })
                .extracting(row -> row.getValues().get(2).longValue())
                .containsExactlyElementsOf(partitions);
    }
}
