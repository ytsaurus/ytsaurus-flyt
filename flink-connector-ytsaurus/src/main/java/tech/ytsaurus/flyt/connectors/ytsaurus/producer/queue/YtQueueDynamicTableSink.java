package tech.ytsaurus.flyt.connectors.ytsaurus.producer.queue;

import javax.annotation.Nullable;

import lombok.Builder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;

@Builder
public class YtQueueDynamicTableSink implements DynamicTableSink {
    private final String proxy;
    private final String queuePath;
    private final CredentialsProvider credentialsProvider;
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    private final DataType physicalRowDataType;
    private final YtQueueWriteMode writeMode;
    private final String valueColumn;
    private final YtQueueWriterOptions writerOptions;
    @Nullable
    private final Integer parallelism;

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> serializer = encodingFormat.createRuntimeEncoder(context, physicalRowDataType);
        YtQueueSinkFunction function = new YtQueueSinkFunction(
                proxy,
                queuePath,
                credentialsProvider,
                serializer,
                (RowType) physicalRowDataType.getLogicalType(),
                writeMode,
                valueColumn,
                writerOptions);
        return SinkFunctionProvider.of(function, parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return YtQueueDynamicTableSink.builder()
                .proxy(proxy)
                .queuePath(queuePath)
                .credentialsProvider(credentialsProvider)
                .encodingFormat(encodingFormat)
                .physicalRowDataType(physicalRowDataType)
                .writeMode(writeMode)
                .valueColumn(valueColumn)
                .writerOptions(writerOptions)
                .parallelism(parallelism)
                .build();
    }

    @Override
    public String asSummaryString() {
        return "YTsaurus Queue Sink";
    }
}
