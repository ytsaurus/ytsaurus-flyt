package tech.ytsaurus.flyt.connectors.ytsaurus.test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import tech.ytsaurus.client.YTsaurusClient;

public class YtClientPool<T extends YTsaurusClient> implements AutoCloseable {
    private final List<T> clientList;
    private final int limit;
    private final Supplier<T> clientSupplier;
    private int current;

    public YtClientPool(Supplier<T> clientSupplier, int limit) {
        this.clientList = new CopyOnWriteArrayList<>();
        this.limit = limit;
        this.clientSupplier = clientSupplier;
        this.current = 0;
    }

    public synchronized T produce() {
        if (limit != -1 && ++current > limit) {
            return clientList.get(current % limit);
        }
        T client = clientSupplier.get();
        clientList.add(client);
        return client;
    }

    @Override
    public synchronized void close() {
        for (T client : clientList) {
            client.close();
        }
    }

    public List<T> getClientList() {
        return clientList;
    }
}
