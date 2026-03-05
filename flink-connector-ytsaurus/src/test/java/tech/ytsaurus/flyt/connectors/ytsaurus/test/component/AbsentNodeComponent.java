package tech.ytsaurus.flyt.connectors.ytsaurus.test.component;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import org.apache.commons.lang3.NotImplementedException;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.ysontree.YTreeNode;

public class AbsentNodeComponent implements NodeComponent {
    @Override
    public CompletableFuture<Boolean> existsNode(String path) {
        throw new NotImplementedException("Node component is absent");
    }

    @Override
    public CompletableFuture<GUID> createNode(CreateNode req) {
        throw new NotImplementedException("Node component is absent");
    }

    @Override
    public CompletableFuture<Void> mountTable(String path, GUID cellId, boolean freeze, boolean waitMounted,
                                              @Nullable Duration requestTimeout) {
        throw new NotImplementedException("Node component is absent");
    }

    @Override
    public CompletableFuture<Void> mountTableAndWaitTablets(MountTable req) {
        throw new NotImplementedException("Node component is absent");
    }

    @Override
    public CompletableFuture<Void> setNode(String path, YTreeNode data) {
        throw new NotImplementedException("Node component is absent");
    }

    @Override
    public CompletableFuture<YTreeNode> getNode(String path) {
        throw new NotImplementedException("Node component is absent");
    }

    @Override
    public CompletableFuture<YTreeNode> getNode(GetNode req) {
        throw new NotImplementedException("Node component is absent");
    }
}
