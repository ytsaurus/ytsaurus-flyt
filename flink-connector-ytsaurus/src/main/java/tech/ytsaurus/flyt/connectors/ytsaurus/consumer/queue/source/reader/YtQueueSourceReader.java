package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.YtQueueRecordDeserializer;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueTrimmedOffsetPolicy;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueReaderOptions;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueueRawRecord;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplit;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplitState;

public final class YtQueueSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                YtQueueRawRecord,
                T,
                YtQueueSplit,
                YtQueueSplitState> {
    private final YtQueuePullerFactory pullerFactory;

    private final YtQueueOffsetCommitter offsetCommitter;

    public YtQueueSourceReader(
            Supplier<? extends YtQueuePuller> pullerSupplier,
            YtQueueRecordDeserializer<T> deserializer,
            YtQueueReaderOptions options,
            Configuration configuration,
            SourceReaderContext context) throws Exception {
        this(
                YtQueuePullerFactory.fromSupplier(pullerSupplier),
                deserializer,
                options,
                YtQueueTrimmedOffsetPolicy.FAIL,
                configuration,
                context,
                YtQueueOffsetCommitter.noOp()
        );
    }

    public YtQueueSourceReader(
            YtQueuePullerFactory pullerFactory,
            YtQueueRecordDeserializer<T> deserializer,
            YtQueueReaderOptions options,
            Configuration configuration,
            SourceReaderContext context) throws Exception {
        this(
                pullerFactory,
                deserializer,
                options,
                YtQueueTrimmedOffsetPolicy.FAIL,
                configuration,
                context,
                YtQueueOffsetCommitter.noOp()
        );
    }

    public YtQueueSourceReader(
            YtQueuePullerFactory pullerFactory,
            YtQueueRecordDeserializer<T> deserializer,
            YtQueueReaderOptions options,
            YtQueueTrimmedOffsetPolicy trimmedOffsetPolicy,
            Configuration configuration,
            SourceReaderContext context) throws Exception {
        this(
                pullerFactory,
                deserializer,
                options,
                trimmedOffsetPolicy,
                configuration,
                context,
                YtQueueOffsetCommitter.noOp()
        );
    }

    public YtQueueSourceReader(
            Supplier<? extends YtQueuePuller> pullerSupplier,
            YtQueueRecordDeserializer<T> deserializer,
            YtQueueReaderOptions options,
            Configuration configuration,
            SourceReaderContext context,
            YtQueueOffsetCommitter offsetCommitter) throws Exception {
        this(
                YtQueuePullerFactory.fromSupplier(pullerSupplier),
                deserializer,
                options,
                YtQueueTrimmedOffsetPolicy.FAIL,
                configuration,
                context,
                offsetCommitter
        );
    }

    private YtQueueSourceReader(
            YtQueuePullerFactory pullerFactory,
            YtQueueRecordDeserializer<T> deserializer,
            YtQueueReaderOptions options,
            YtQueueTrimmedOffsetPolicy trimmedOffsetPolicy,
            Configuration configuration,
            SourceReaderContext context,
            YtQueueOffsetCommitter offsetCommitter) throws Exception {
        super(
                splitReaderSupplier(pullerFactory, options, trimmedOffsetPolicy),
                new YtQueueRecordEmitter<>(deserializer),
                Objects.requireNonNull(configuration, "configuration"),
                Objects.requireNonNull(context, "context")
        );
        this.pullerFactory = Objects.requireNonNull(pullerFactory, "pullerFactory");
        this.offsetCommitter = Objects.requireNonNull(offsetCommitter, "offsetCommitter");
        try {
            deserializer.open(context);
        } catch (Exception | Error failure) {
            try {
                super.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            closeAfterFailure(this.offsetCommitter, failure);
            closeAfterFailure(this.pullerFactory, failure);
            throw failure;
        }
    }

    @Override
    public List<YtQueueSplit> snapshotState(long checkpointId) {
        List<YtQueueSplit> splits = super.snapshotState(checkpointId);
        offsetCommitter.snapshotState(checkpointId, List.copyOf(splits));
        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        offsetCommitter.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        offsetCommitter.notifyCheckpointAborted(checkpointId);
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        try {
            super.close();
        } catch (Exception e) {
            failure = e;
        }
        try {
            offsetCommitter.close();
        } catch (Exception e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        try {
            pullerFactory.close();
        } catch (Exception e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    protected void onSplitFinished(Map<String, YtQueueSplitState> finishedSplitIds) {
    }

    @Override
    protected YtQueueSplitState initializedState(YtQueueSplit split) {
        return new YtQueueSplitState(split);
    }

    @Override
    protected YtQueueSplit toSplitType(String splitId, YtQueueSplitState splitState) {
        YtQueueSplit split = splitState.toSplit();
        if (!split.splitId().equals(splitId)) {
            throw new IllegalStateException("Split state identity changed");
        }
        return split;
    }

    private static Supplier<SplitReader<YtQueueRawRecord, YtQueueSplit>> splitReaderSupplier(
            Supplier<? extends YtQueuePuller> pullerSupplier,
            YtQueueReaderOptions options,
            YtQueueTrimmedOffsetPolicy trimmedOffsetPolicy) {
        Objects.requireNonNull(pullerSupplier, "pullerSupplier");
        Objects.requireNonNull(options, "options");
        return () -> new YtQueueSplitReader(pullerSupplier, options, trimmedOffsetPolicy);
    }

    private static void closeAfterFailure(AutoCloseable closeable, Throwable failure) {
        try {
            closeable.close();
        } catch (Exception closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }
}
