package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.metadata;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class YTsaurusQueueMetadataProviderTest {
    @Test
    void readsQueueIdentityAndTabletCountInOneRequestAndClosesClient() {
        YTsaurusClient client = mock(YTsaurusClient.class);
        YTreeNode response = YTree.builder()
                .beginAttributes()
                .key("id").value("11-22-33-44")
                .key("tablet_count").value(2)
                .endAttributes()
                .entity()
                .build();
        when(client.getNode(any(GetNode.class))).thenReturn(CompletableFuture.completedFuture(response));
        YTsaurusQueueMetadataProvider provider = new YTsaurusQueueMetadataProvider(client, "//home/queue");

        YtQueueMetadata metadata = provider.getMetadata();
        provider.close();

        assertThat(metadata).isEqualTo(new YtQueueMetadata("11-22-33-44", 2));
        ArgumentCaptor<GetNode> request = ArgumentCaptor.forClass(GetNode.class);
        verify(client).getNode(request.capture());
        assertThat(request.getValue().getPath().toString()).isEqualTo("//home/queue");
        assertThat(request.getValue().getAttributes())
                .hasValue(List.of("id", "tablet_count"));
        YTreeNode readFrom = YTree.builder()
                .apply(request.getValue().getMasterReadOptions().orElseThrow()::toTree)
                .build();
        assertThat(readFrom.stringValue()).isEqualTo("leader");
        verify(client).close();
    }
}
