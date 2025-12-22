package tech.ytsaurus.flyt.connectors.ytsaurus.test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nullable;

import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.ysontree.YTreeNode;

import tech.ytsaurus.flyt.connectors.ytsaurus.test.component.NodeComponent;
import tech.ytsaurus.flyt.connectors.ytsaurus.test.component.TransactionComponent;

/**
 * Splits original YTClient into several components (modularization) for convenient testing
 */
public class TestYtClient<N extends NodeComponent, T extends TransactionComponent> extends YTsaurusClient {
    private final N nodeComponent;
    private final T transactionComponent;

    public TestYtClient(N nodeComponent, T transactionComponent) {
        super(null, "test_mock_cluster.ytsaurus.net", YTsaurusClientAuth.builder()
                .setUser("test_mock")
                .setToken("test_mock")
                .build());
        this.nodeComponent = nodeComponent;
        this.transactionComponent = transactionComponent;
    }

    @Override
    public CompletableFuture<Boolean> existsNode(String path) {
        return nodeComponent.existsNode(path);
    }

    @Override
    public CompletableFuture<YTreeNode> getNode(GetNode req) {
        return super.getNode(req);
    }

    @Override
    public CompletableFuture<GUID> createNode(CreateNode req) {
        return nodeComponent.createNode(req);
    }

    @Override
    public CompletableFuture<Void> mountTable(String path) {
        return nodeComponent.mountTable(path, null, false, false, null);
    }

    @Override
    public CompletableFuture<Void> mountTable(String path,
                                              GUID cellId,
                                              boolean freeze,
                                              boolean waitMounted,
                                              @Nullable Duration requestTimeout) {
        return nodeComponent.mountTable(path, cellId, freeze, waitMounted, requestTimeout);
    }

    @Override
    public CompletableFuture<Void> setNode(String path, YTreeNode data) {
        return nodeComponent.setNode(path, data);
    }

    @Override
    public CompletableFuture<ApiServiceTransaction> startTransaction(StartTransaction startTransaction) {
        return transactionComponent.startTransaction(startTransaction);
    }

    @Override
    public CompletableFuture<YTreeNode> getNode(String path) {
        return nodeComponent.getNode(path);
    }

    public N nodes() {
        return nodeComponent;
    }

    public T transactions() {
        return transactionComponent;
    }
}
