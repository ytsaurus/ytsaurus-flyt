package tech.ytsaurus.flyt.connectors.ytsaurus.test.component;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.ysontree.YTreeNode;

public interface NodeComponent {
    CompletableFuture<Boolean> existsNode(String path);

    CompletableFuture<GUID> createNode(CreateNode req);

    CompletableFuture<Void> mountTable(String path,
                                       GUID cellId,
                                       boolean freeze,
                                       boolean waitMounted,
                                       @Nullable Duration requestTimeout);

    CompletableFuture<Void> setNode(String path, YTreeNode data);

    CompletableFuture<YTreeNode> getNode(String path);

    CompletableFuture<YTreeNode> getNode(GetNode req);
}
