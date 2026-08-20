package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.table;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import lombok.Builder;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.YtQueueColumnValueDeserializer;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.YtQueueRecordDeserializer;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueColumnModeOptions;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueReadMode;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueStartupMode;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueTrimmedOffsetPolicy;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueReaderOptions;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueSource;

@Builder
public class YtQueueDynamicTableSource implements ScanTableSource {
    private final String proxy;

    private final String queuePath;

    private final CredentialsProvider credentialsProvider;

    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    private final DataType physicalRowDataType;

    private final YtQueueStartupMode startupMode;

    @Nullable
    private final List<Long> specificOffsets;

    private final YtQueueTrimmedOffsetPolicy trimmedOffsetPolicy;

    private final YtQueueReaderOptions readerOptions;

    private final YtQueueReadMode readMode;

    @Nullable
    private final YtQueueColumnModeOptions columnModeOptions;

    private final Duration partitionDiscoveryInterval;

    @Nullable
    private final Integer parallelism;

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        DeserializationSchema<RowData> deserializer =
                decodingFormat.createRuntimeDecoder(context, physicalRowDataType);
        TypeInformation<RowData> producedType = context.createTypeInformation(physicalRowDataType);

        YtQueueSource<RowData> source = YtQueueSource.<RowData>builder()
                .proxy(proxy)
                .queuePath(queuePath)
                .credentialsProvider(credentialsProvider)
                .recordDeserializer(createRecordDeserializer(deserializer))
                .producedType(producedType)
                .startupMode(startupMode)
                .specificOffsets(specificOffsets)
                .trimmedOffsetPolicy(trimmedOffsetPolicy)
                .readerOptions(readerOptions)
                .discoveryInterval(partitionDiscoveryInterval)
                .build();
        return SourceProvider.of(source, parallelism);
    }

    private YtQueueRecordDeserializer<RowData> createRecordDeserializer(
            DeserializationSchema<RowData> deserializer) {
        if (readMode != YtQueueReadMode.COLUMN) {
            return new YtQueueRowDataDeserializer(deserializer);
        }
        YtQueueColumnModeOptions options = Objects.requireNonNull(
                columnModeOptions,
                "columnModeOptions are required for COLUMN read mode");
        return new YtQueueColumnValueDeserializer<>(
                deserializer,
                options.getValueColumn(),
                options.getCodecColumn());
    }

    @Override
    public DynamicTableSource copy() {
        return YtQueueDynamicTableSource.builder()
                .proxy(proxy)
                .queuePath(queuePath)
                .credentialsProvider(credentialsProvider)
                .decodingFormat(decodingFormat)
                .physicalRowDataType(physicalRowDataType)
                .startupMode(startupMode)
                .specificOffsets(specificOffsets)
                .trimmedOffsetPolicy(trimmedOffsetPolicy)
                .readerOptions(readerOptions)
                .readMode(readMode)
                .columnModeOptions(columnModeOptions)
                .partitionDiscoveryInterval(partitionDiscoveryInterval)
                .parallelism(parallelism)
                .build();
    }

    @Override
    public String asSummaryString() {
        return "YTsaurus Queue";
    }
}
