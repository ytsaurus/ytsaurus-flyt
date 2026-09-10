package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.table;

import java.util.Optional;

import javax.annotation.Nullable;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.connector.ParallelismProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;

/** Source provider that exposes the optional scan parallelism. */
final class YtQueueSourceProvider implements SourceProvider, ParallelismProvider {
    private final Source<RowData, ?, ?> source;

    @Nullable
    private final Integer parallelism;

    YtQueueSourceProvider(Source<RowData, ?, ?> source, @Nullable Integer parallelism) {
        this.source = source;
        this.parallelism = parallelism;
    }

    @Override
    public Source<RowData, ?, ?> createSource() {
        return source;
    }

    @Override
    public boolean isBounded() {
        return source.getBoundedness() == Boundedness.BOUNDED;
    }

    @Override
    public Optional<Integer> getParallelism() {
        return Optional.ofNullable(parallelism);
    }
}
