package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.initializer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.GetTabletInfos;
import tech.ytsaurus.client.request.TabletInfo;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueStartupMode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueTestFixtures.metadata;

class YTsaurusQueueOffsetInitializerTest {
    private static final String QUEUE_PATH = "//home/test/queue";

    @Test
    void readsTrimmedRowCountsForAllPartitionsInOneRequest() {
        YTsaurusClient client = mock(YTsaurusClient.class);
        when(client.getTabletInfos(any(GetTabletInfos.class))).thenReturn(
                CompletableFuture.completedFuture(List.of(
                        new TabletInfo(100, 17, 0, List.of()),
                        new TabletInfo(200, 23, 0, List.of()))));
        YTsaurusQueueOffsetInitializer initializer =
                new YTsaurusQueueOffsetInitializer(
                        client,
                        QUEUE_PATH,
                        YtQueueStartupMode.EARLIEST);

        Map<Integer, Long> offsets = initializer.getInitialOffsets(
                metadata("11-22-33-44", 3),
                List.of(0, 2));
        initializer.close();

        assertThat(offsets).containsExactly(entry(0, 17L), entry(2, 23L));
        ArgumentCaptor<GetTabletInfos> request = ArgumentCaptor.forClass(GetTabletInfos.class);
        verify(client).getTabletInfos(request.capture());
        assertThat(request.getValue().getArgumentsLogString())
                .contains("Path: //home/test/queue")
                .contains("TabletIndexes: [0, 2]");
        verify(client).close();
        verifyNoMoreInteractions(client);
    }

    @Test
    void readsCurrentTotalRowCountsForLatestMode() {
        YTsaurusClient client = mock(YTsaurusClient.class);
        when(client.getTabletInfos(any(GetTabletInfos.class)))
                .thenReturn(CompletableFuture.completedFuture(List.of(
                        new TabletInfo(100, 17, 0, List.of()),
                        new TabletInfo(200, 23, 0, List.of()))))
                .thenReturn(CompletableFuture.completedFuture(List.of(
                        new TabletInfo(350, 0, 0, List.of()))));
        YTsaurusQueueOffsetInitializer initializer =
                new YTsaurusQueueOffsetInitializer(
                        client,
                        QUEUE_PATH,
                        YtQueueStartupMode.LATEST);

        Map<Integer, Long> offsets = initializer.getInitialOffsets(
                metadata("11-22-33-44", 2),
                List.of(0, 1));
        Map<Integer, Long> addedPartitionOffset = initializer.getInitialOffsets(
                metadata("11-22-33-44", 3),
                List.of(2));

        assertThat(offsets).containsExactly(entry(0, 100L), entry(1, 200L));
        assertThat(addedPartitionOffset).containsExactly(entry(2, 350L));
        verify(client, times(2)).getTabletInfos(any(GetTabletInfos.class));
        verifyNoMoreInteractions(client);
    }

    @Test
    void rejectsIncompleteTabletInfoResponse() {
        YTsaurusClient client = mock(YTsaurusClient.class);
        when(client.getTabletInfos(any(GetTabletInfos.class))).thenReturn(
                CompletableFuture.completedFuture(List.of(
                        new TabletInfo(100, 17, 0, List.of()))));
        YTsaurusQueueOffsetInitializer initializer =
                new YTsaurusQueueOffsetInitializer(
                        client,
                        QUEUE_PATH,
                        YtQueueStartupMode.EARLIEST);

        assertThatThrownBy(() -> initializer.getInitialOffsets(
                metadata("11-22-33-44", 3),
                List.of(0, 2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Expected 2 tablet infos, got 1");
        verify(client).getTabletInfos(any(GetTabletInfos.class));
    }

    @Test
    void rejectsPartitionOutsideDiscoveredTopology() {
        YTsaurusClient client = mock(YTsaurusClient.class);
        YTsaurusQueueOffsetInitializer initializer =
                new YTsaurusQueueOffsetInitializer(
                        client,
                        QUEUE_PATH,
                        YtQueueStartupMode.EARLIEST);

        assertThatThrownBy(() -> initializer.getInitialOffsets(
                metadata("queue-id", 2),
                List.of(0, 2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partitionIndex");
        verifyNoInteractions(client);
    }
}
