package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.Builder;
import lombok.Getter;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.apache.flink.util.function.SerializableSupplier;

import tech.ytsaurus.flyt.connectors.datametrics.DataMetricsConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardingConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.TrackableField;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtTableAttributes;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;

/** YTsaurus dynamic table sink backed by Flink Sink V2. */
@Builder
@Getter
public class YtDynamicTableSink implements DynamicTableSink {
    private final DataType type;
    private final RowDataToYtListConverters ytConverters;
    private final TrackableField trackableField;
    private final ComplexYtPath path;
    private final @Nullable Integer parallelism;
    private String ysonSchemaString;
    private PartitionConfig partitionConfig;
    private CredentialsProvider credentialsProvider;
    private boolean eagerInitialization;

    @Builder.Default
    private @Nonnull YtTableAttributes tableAttributes = YtTableAttributes.empty();

    private SerializableSupplier<RetryStrategy> retryStrategy;
    private ReshardingConfig reshardingConfig;
    private YtWriterOptions ytWriterOptions;

    @Nullable
    private DataMetricsConfig dataMetricsConfig;

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkV2Provider.of(createSink(), parallelism);
    }

    private YtSink<RowData> createSink() {
        return new YtSink<>(
                type,
                ytConverters,
                value -> value,
                trackableField,
                path,
                ysonSchemaString,
                partitionConfig,
                credentialsProvider,
                eagerInitialization,
                tableAttributes,
                retryStrategy,
                reshardingConfig,
                ytWriterOptions,
                dataMetricsConfig);
    }

    @Override
    public DynamicTableSink copy() {
        return YtDynamicTableSink.builder()
                .partitionConfig(partitionConfig)
                .credentialsProvider(credentialsProvider)
                .ysonSchemaString(ysonSchemaString)
                .path(path)
                .trackableField(trackableField)
                .ytConverters(ytConverters)
                .type(type)
                .parallelism(parallelism)
                .retryStrategy(retryStrategy)
                .eagerInitialization(eagerInitialization)
                .tableAttributes(tableAttributes)
                .reshardingConfig(reshardingConfig)
                .ytWriterOptions(ytWriterOptions)
                .dataMetricsConfig(dataMetricsConfig)
                .build();
    }

    @Override
    public String asSummaryString() {
        return "Write to " + path;
    }
}
