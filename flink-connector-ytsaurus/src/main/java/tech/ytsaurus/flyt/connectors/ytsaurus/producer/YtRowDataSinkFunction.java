package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Instant;

import javax.annotation.Nullable;

import org.apache.flink.util.Preconditions;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.apache.flink.util.function.SerializableSupplier;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.flyt.connectors.ytsaurus.YtConnectorInfo;
import tech.ytsaurus.flyt.locks.api.LocksProvider;
import tech.ytsaurus.flyt.locks.api.LocksProviderChooser;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import tech.ytsaurus.flyt.connectors.datametrics.DataMetricsConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardingConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.TrackableField;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtTableAttributes;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.OAuthCredentialsConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.utils.RowTypeUtils;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.utils.project.info.ProjectInfoUtils;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.ConverterUtils;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtUtils;


@Slf4j
public class YtRowDataSinkFunction extends RichSinkFunction<RowData> implements CheckpointedFunction {
    private static final long serialVersionUID = 1L;

    private final ComplexYtPath path;
    private final String ysonSchemaString;
    private final RowDataToYtListConverters.RowDataToYtMapConverter ytConverters;
    private final LogicalType logicalType;
    private final DataType originalType;
    private PartitionConfig partitionConfig;
    private int partitionKeyColumnIndex;
    private OAuthCredentialsConfig credentialsConfig;
    private TrackableField trackableField;
    private YtDynamicTableWriterPool pool;
    private final boolean eagerInitialization;
    private YtTableAttributes tableAttributes;
    private SerializableSupplier<RetryStrategy> retryStrategy;

    private ReshardingConfig reshardingConfig;
    private YtWriterOptions ytWriterOptions;

    @Nullable
    private DataMetricsConfig dataMetricsConfig;

    @SuppressWarnings("checkstyle:ParameterNumber")
    public YtRowDataSinkFunction(RowDataToYtListConverters ytConverters,
                                 ComplexYtPath path,
                                 DataType type,
                                 String ysonSchemaString,
                                 SerializableSupplier<RetryStrategy> retryStrategy,
                                 boolean eagerInitialization,
                                 YtTableAttributes tableAttributes,
                                 ReshardingConfig reshardingConfig,
                                 YtWriterOptions ytWriterOptions) {
        if (path.getBasePath().isEmpty()) {
            throw new IllegalArgumentException("YT path cannot be empty");
        }
        YTreeNode schemaNode = ConverterUtils.toWriteNode(YTreeTextSerializer.deserialize(ysonSchemaString));
        this.ytConverters = ytConverters.createConverter(type.getLogicalType(), schemaNode);
        this.path = path;
        this.logicalType = type.getLogicalType();
        this.originalType = type;
        this.ysonSchemaString = ysonSchemaString;
        this.eagerInitialization = eagerInitialization;
        this.retryStrategy = retryStrategy;
        this.partitionKeyColumnIndex = -1;
        this.tableAttributes = tableAttributes;
        this.reshardingConfig = reshardingConfig;
        this.ytWriterOptions = ytWriterOptions;
    }

    public void enablePartitioning(PartitionConfig partitionConfig) {
        Preconditions.checkNotNull(partitionConfig);
        this.partitionConfig = partitionConfig;
        // Get partition key index from Flink last select schema
        // Assuming that field order in the flink schema is the same
        this.partitionKeyColumnIndex = RowTypeUtils.findColumnByName(
                logicalType,
                partitionConfig.getPartitionKey());
        if (this.partitionKeyColumnIndex == -1) {
            throw new IllegalArgumentException(String.format(
                    "Partition key '%s' is not found in last select %s",
                    partitionConfig.getPartitionKey(),
                    logicalType));
        }
    }

    public YtRowDataSinkFunction withCredentials(CredentialsProvider credentialsProvider) {
        Preconditions.checkNotNull(credentialsProvider);
        this.credentialsConfig = credentialsProvider.getCredentials(path.getClusterName());
        if (credentialsConfig.getUsername() == null) {
            throw new IllegalArgumentException(
                    "Could not find username to connect to YT as. Please, check your credentials");
        }
        if (credentialsConfig.getToken() == null) {
            throw new IllegalArgumentException(String.format(
                    "Could not find token to connect to YT (username=%s). Please, check your credentials",
                    credentialsConfig.getUsername()));
        }
        return this;
    }

    public YtRowDataSinkFunction withTrackableField(TrackableField field) {
        this.trackableField = field;
        return this;
    }

    public YtRowDataSinkFunction withDataMetricsConfig(DataMetricsConfig config) {
        this.dataMetricsConfig = config;
        return this;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) {
        for (YtDynamicTableWriter writer : pool.getWriters()) {
            writer.snapshotState(context.getCheckpointId());
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        //ignore
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        LocksProvider locksProvider = LocksProviderChooser.chooseAndConfigureLocksProvider(
                ytWriterOptions.getLocksConfig().getLocksProviderName(),
                ytWriterOptions.getLocksConfig().getConfig());

        this.pool = new YtDynamicTableWriterPool(
                null,  // cache - will be created by constructor
                this::makeYtClient,
                ytConverters,
                path,
                ysonSchemaString,
                trackableField,
                retryStrategy.get(),
                getRuntimeContext(),
                tableAttributes,
                reshardingConfig,
                ytWriterOptions,
                locksProvider,
                originalType,
                dataMetricsConfig);
        if (eagerInitialization) {
            if (partitionConfig != null) {
                log.info("Eager initialize map node for partition tables at {}", path.getBasePath());
                pool.createBasePathMapNode();
            } else {
                log.info("Eager initialize target table at {} ({})", path.getBasePath(), path.getTableName());
                pool.createFullPathTable();
            }
        }
        ProjectInfoUtils.registerProjectInFlinkMetrics(YtConnectorInfo.MAVEN_NAME,
                YtConnectorInfo.VERSION,
                () -> getRuntimeContext().getMetricGroup());
    }

    @SneakyThrows
    @Override
    public void invoke(RowData value, Context context) {
        if (value == null) {
            return;
        }
        pool.getOrAcquire(dispatchQuery(value)).write(value);
    }

    @Override
    public void finish() throws Exception {
        log.info("Finish sink function for table: {}", path);
        pool.finish();
    }

    @Override
    public void close() {
        if (pool != null) {
            pool.close();
        }
    }

    private YTsaurusClient makeYtClient() {
        var clientBuilder = YtUtils.makeYtClientBuilder(path, credentialsConfig);
        if (ytWriterOptions.getProxyRole() != null) {
            log.info("Using proxy role: {} for table: {}", ytWriterOptions.getProxyRole(), path.getBasePath());
            clientBuilder.setProxyRole(ytWriterOptions.getProxyRole());
        }
        return clientBuilder.build();
    }

    private WriterClassifier dispatchQuery(RowData data) {
        if (partitionKeyColumnIndex != -1) {
            Instant instant = partitionConfig.getConverter().convert(data, partitionKeyColumnIndex);
            return WriterClassifier.partition(instant, partitionConfig);
        } else {
            return WriterClassifier.plain(path.getBaseTableName());
        }
    }
}
