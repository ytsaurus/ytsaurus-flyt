package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import javax.annotation.Nullable;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;

/** Flink's DynamicTableSink interface adapts SQL INSERTs to a static YT destination. */
public final class YtStaticDynamicTableSink implements DynamicTableSink {
    private final YtStaticSink<RowData> sink;
    @Nullable
    private final Integer parallelism;

    public YtStaticDynamicTableSink(YtStaticSink<RowData> sink, @Nullable Integer parallelism) {
        this.sink = sink;
        this.parallelism = parallelism;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkV2Provider.of(sink, parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new YtStaticDynamicTableSink(sink, parallelism);
    }

    @Override
    public String asSummaryString() {
        return "Static YT append to " + sink.path;
    }
}
