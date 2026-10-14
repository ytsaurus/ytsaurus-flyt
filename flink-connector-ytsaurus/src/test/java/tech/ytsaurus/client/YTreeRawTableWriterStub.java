package tech.ytsaurus.client;

import java.util.concurrent.CompletableFuture;

import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rows.YTreeWireRowSerializer;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.rows.YTreeMapNodeSerializer;
import tech.ytsaurus.ysontree.YTreeMapNode;

/** Runs the SDK's real raw serialization path, replacing only the network transport. */
public final class YTreeRawTableWriterStub extends TableWriterImpl<YTreeMapNode> {
    private int acceptedPayloads;

    public YTreeRawTableWriterStub() {
        super(WriteTable.builder(YTreeMapNode.class).setPath(YPath.simple("//tmp/test"))
                .setNeedRetries(false).build(), DefaultSerializationResolver.getInstance());
        // This is the serializer the SDK installs after reading the upload metadata.
        tableRowsSerializer = new TableRowsWireSerializer<>(
                YTreeWireRowSerializer.forClass(new YTreeMapNodeSerializer()));
    }

    @Override
    public boolean write(byte[] attachment) {
        acceptedPayloads++;
        return true;
    }

    @Override
    public CompletableFuture<?> close() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void cancel() {
        // No network stream is opened by this test double.
    }

    public int getAcceptedPayloads() {
        return acceptedPayloads;
    }
}
