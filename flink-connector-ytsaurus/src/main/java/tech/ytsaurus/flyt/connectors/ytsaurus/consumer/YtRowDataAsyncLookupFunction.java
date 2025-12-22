package tech.ytsaurus.flyt.connectors.ytsaurus.consumer;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;

import tech.ytsaurus.flyt.connectors.ytsaurus.YtConnectorInfo;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.utils.project.info.ProjectInfoUtils;

@Slf4j
public class YtRowDataAsyncLookupFunction extends AsyncLookupFunction {

    private final LookupFunction lookupFunction;

    private transient ExecutorService lookupExecutor;

    public YtRowDataAsyncLookupFunction(LookupFunction lookupFunction) {
        this.lookupFunction = lookupFunction;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        lookupFunction.open(context);
        lookupExecutor = Executors.newCachedThreadPool();
        ProjectInfoUtils.registerProjectInFlinkMetrics(YtConnectorInfo.MAVEN_NAME,
                YtConnectorInfo.VERSION,
                context::getMetricGroup);
    }

    @Override
    public void close() throws Exception {
        lookupFunction.close();
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return lookupFunction.lookup(keyRow);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, lookupExecutor);
    }
}
