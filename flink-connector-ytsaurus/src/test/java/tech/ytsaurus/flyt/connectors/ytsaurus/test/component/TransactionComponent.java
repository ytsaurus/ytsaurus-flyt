package tech.ytsaurus.flyt.connectors.ytsaurus.test.component;

import java.util.concurrent.CompletableFuture;

import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.request.StartTransaction;

public interface TransactionComponent {
    CompletableFuture<ApiServiceTransaction> startTransaction(StartTransaction startTransaction);
}
