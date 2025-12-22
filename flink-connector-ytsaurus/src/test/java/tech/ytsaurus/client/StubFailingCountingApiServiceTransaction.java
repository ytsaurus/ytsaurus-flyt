package tech.ytsaurus.client;

import java.time.Instant;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import lombok.SneakyThrows;
import tech.ytsaurus.client.request.AbstractModifyRowsRequest;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.YtTimestamp;

/**
 * Stub transaction that simulates failures on commit.
 * This IS NOT a part of official YT/YTSaurus client, but needs to be here because of original constructor visibility
 */
public class StubFailingCountingApiServiceTransaction extends ApiServiceTransaction {
    private final AtomicLong rowCount;
    private final Iterator<Boolean> successGenerator;
    private final Consumer<Long> committedRowsHook;
    private final Consumer<StubFailingCountingApiServiceTransaction> preCommitHook;
    private final Runnable postCommitHook;

    @SneakyThrows
    public StubFailingCountingApiServiceTransaction(Random random,
                                                    Iterator<Boolean> successGenerator,
                                                    Consumer<Long> committedRowsHook,
                                                    Consumer<StubFailingCountingApiServiceTransaction> preCommitHook,
                                                    Runnable postCommitHook) {
        super(null, GUID.create(random), YtTimestamp.fromInstant(Instant.now()), false, false, false, null, null);
        this.rowCount = new AtomicLong(0);
        this.successGenerator = successGenerator;
        this.committedRowsHook = committedRowsHook;
        this.postCommitHook = postCommitHook;
        this.preCommitHook = preCommitHook;
    }

    @Override
    public CompletableFuture<Void> modifyRows(AbstractModifyRowsRequest<?, ?> request) {
        throw new UnsupportedOperationException("modifyRows of this type is not supported");
    }

    @Override
    public CompletableFuture<Void> modifyRows(AbstractModifyRowsRequest.Builder<?, ?> request) {
        return CompletableFuture.runAsync(() -> {
            if (!(request instanceof ModifyRowsRequest.Builder)) {
                throw new IllegalStateException(
                        "Unsupported request type. " +
                                "Expected '" + ModifyRowsRequest.class + "', " +
                                "but got '" + request.getClass() + "'");
            }
            long rowSizes =
                    ((tech.ytsaurus.client.request.ModifyRowsRequest) request.build()).getRowModificationTypes().size();
            rowCount.addAndGet(rowSizes);
        });
    }

    @Override
    public CompletableFuture<Void> commit() {
        return CompletableFuture.runAsync(() -> {
            preCommitHook.accept(this);
            try {
                if (!successGenerator.next()) {
                    committedRowsHook.accept(0L);
                    throw new IllegalStateException("Intended @" + getId());
                }
                committedRowsHook.accept(rowCount.get());
            } finally {
                postCommitHook.run();
            }
        });
    }
}
