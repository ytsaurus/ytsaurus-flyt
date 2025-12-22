package tech.ytsaurus.flyt.connectors.ytsaurus.test;

import java.util.function.Supplier;

import org.junit.jupiter.api.Assertions;

import tech.ytsaurus.flyt.connectors.ytsaurus.test.component.StubFailingCountingTransactionComponent;

public class CountingTestYtClientPool extends YtClientPool<TestYtClient<?, StubFailingCountingTransactionComponent>> {
    public CountingTestYtClientPool(
            Supplier<TestYtClient<?, StubFailingCountingTransactionComponent>> supplier,
            int limit) {
        super(supplier, limit);
    }

    public static CountingTestYtClientPool ofSingle(TestYtClient<?, StubFailingCountingTransactionComponent> client) {
        return new CountingTestYtClientPool(() -> client, 1);
    }

    public synchronized long getCommittedRows() {
        return getClientList().stream()
                .map(it -> it.transactions().getCommittedRows())
                .reduce(0L, Long::sum);
    }

    @Override
    public synchronized void close() {
        super.close();
        for (TestYtClient<?, StubFailingCountingTransactionComponent> client : getClientList()) {
            Assertions.assertEquals(0, client.transactions().getPendingTransactionsCount());
        }
    }
}
