package tech.ytsaurus.flyt.connectors.ytsaurus.test.component;

import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.StubFailingCountingApiServiceTransaction;
import tech.ytsaurus.client.request.StartTransaction;

public class StubFailingCountingTransactionComponent implements TransactionComponent {
    private final AtomicLong committedRows = new AtomicLong(0);
    private final Phaser phaser = new Phaser(1);
    private final Random random;
    private final Iterator<Boolean> successIterator;
    private final Consumer<StubFailingCountingApiServiceTransaction> preCommitHook;

    public StubFailingCountingTransactionComponent(Random random,
                                                   Iterator<Boolean> successIterator,
                                                   Consumer<StubFailingCountingApiServiceTransaction> preCommitHook) {
        this.random = random;
        this.successIterator = successIterator;
        this.preCommitHook = preCommitHook;
    }

    @Override
    public CompletableFuture<ApiServiceTransaction> startTransaction(StartTransaction startTransaction) {
        phaser.register();
        return CompletableFuture.completedFuture(
                new StubFailingCountingApiServiceTransaction(
                        random,
                        successIterator,
                        committedRows::addAndGet,
                        preCommitHook,
                        phaser::arrive));
    }

    public long getCommittedRows() {
        return committedRows.get();
    }

    public int getPendingTransactionsCount() {
        return phaser.getUnarrivedParties() - 1;
    }
}
