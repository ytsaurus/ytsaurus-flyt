package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.io.IOException;
import java.time.Instant;

import javax.annotation.Nullable;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.apache.flink.util.function.SerializableFunction;
import org.apache.flink.util.function.SerializableSupplier;

import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.flyt.connectors.datametrics.DataMetricsConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.YtConnectorInfo;
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
import tech.ytsaurus.flyt.locks.api.LocksProvider;
import tech.ytsaurus.flyt.locks.api.LocksProviderChooser;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

/** Flink Sink V2 adapter around the YTsaurus writer core. */
final class YtSink<T> implements Sink<T> {
    private static final long serialVersionUID = 1L;

    private final DataType type;
    private final RowDataToYtListConverters ytConverters;
    private final SerializableFunction<T, RowData> inputConverter;
    private final TrackableField trackableField;
    private final ComplexYtPath path;
    private final String ysonSchemaString;
    private final PartitionConfig partitionConfig;
    private final CredentialsProvider credentialsProvider;
    private final boolean eagerInitialization;
    private final YtTableAttributes tableAttributes;
    private final SerializableSupplier<RetryStrategy> retryStrategy;
    private final ReshardingConfig reshardingConfig;
    private final YtWriterOptions ytWriterOptions;

    @Nullable
    private final DataMetricsConfig dataMetricsConfig;

    @SuppressWarnings("checkstyle:ParameterNumber")
    YtSink(
            DataType type,
            RowDataToYtListConverters ytConverters,
            SerializableFunction<T, RowData> inputConverter,
            TrackableField trackableField,
            ComplexYtPath path,
            String ysonSchemaString,
            PartitionConfig partitionConfig,
            CredentialsProvider credentialsProvider,
            boolean eagerInitialization,
            YtTableAttributes tableAttributes,
            SerializableSupplier<RetryStrategy> retryStrategy,
            ReshardingConfig reshardingConfig,
            YtWriterOptions ytWriterOptions,
            @Nullable DataMetricsConfig dataMetricsConfig) {
        this.type = type;
        this.ytConverters = ytConverters;
        this.inputConverter = inputConverter;
        this.trackableField = trackableField;
        this.path = path;
        this.ysonSchemaString = ysonSchemaString;
        this.partitionConfig = partitionConfig;
        this.credentialsProvider = credentialsProvider;
        this.eagerInitialization = eagerInitialization;
        this.tableAttributes = tableAttributes;
        this.retryStrategy = retryStrategy;
        this.reshardingConfig = reshardingConfig;
        this.ytWriterOptions = ytWriterOptions;
        this.dataMetricsConfig = dataMetricsConfig;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        try {
            return new Writer(context.metricGroup());
        } catch (RuntimeException e) {
            throw new IOException("Failed to initialize YTsaurus sink writer for " + path, e);
        }
    }

    private final class Writer implements SinkWriter<T> {
        private final LogicalType logicalType;
        private final OAuthCredentialsConfig credentialsConfig;
        private final int partitionKeyColumnIndex;
        private final YtDynamicTableWriterPool pool;

        private Writer(MetricGroup metricGroup) {
            if (path.getBasePath().isEmpty()) {
                throw new IllegalArgumentException("YT path cannot be empty");
            }

            YTreeNode schemaNode = ConverterUtils.toWriteNode(YTreeTextSerializer.deserialize(ysonSchemaString));
            this.logicalType = type.getLogicalType();
            RowDataToYtListConverters.RowDataToYtMapConverter ytConverter =
                    ytConverters.createConverter(logicalType, schemaNode);
            this.credentialsConfig = credentialsProvider.getCredentials(path.getClusterName());
            validateCredentials(credentialsConfig);
            this.partitionKeyColumnIndex = resolvePartitionKeyColumnIndex();

            LocksProvider locksProvider = LocksProviderChooser.chooseAndConfigureLocksProvider(
                    ytWriterOptions.getLocksConfig().getLocksProviderName(),
                    ytWriterOptions.getLocksConfig().getConfig());

            this.pool = new YtDynamicTableWriterPool(
                    null,
                    this::makeYtClient,
                    ytConverter,
                    path,
                    ysonSchemaString,
                    trackableField,
                    retryStrategy.get(),
                    metricGroup,
                    tableAttributes,
                    reshardingConfig,
                    ytWriterOptions,
                    locksProvider,
                    type,
                    dataMetricsConfig);

            if (eagerInitialization) {
                if (partitionConfig != null) {
                    pool.createBasePathMapNode();
                } else {
                    pool.createFullPathTable();
                }
            }
            ProjectInfoUtils.registerProjectInFlinkMetrics(
                    YtConnectorInfo.MAVEN_NAME,
                    YtConnectorInfo.VERSION,
                    () -> metricGroup);
        }

        @Override
        public void write(T value, Context context) throws IOException, InterruptedException {
            if (value == null) {
                return;
            }
            RowData row = inputConverter.apply(value);
            if (row != null) {
                pool.getOrAcquire(dispatchQuery(row)).write(row);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            pool.finish();
        }

        @Override
        public void close() throws Exception {
            pool.close();
        }

        private int resolvePartitionKeyColumnIndex() {
            if (partitionConfig == null) {
                return -1;
            }
            Preconditions.checkNotNull(partitionConfig);
            int columnIndex = RowTypeUtils.findColumnByName(logicalType, partitionConfig.getPartitionKey());
            if (columnIndex == -1) {
                throw new IllegalArgumentException(String.format(
                        "Partition key '%s' is not found in last select %s",
                        partitionConfig.getPartitionKey(),
                        logicalType));
            }
            return columnIndex;
        }

        private WriterClassifier dispatchQuery(RowData data) {
            if (partitionKeyColumnIndex != -1) {
                Instant instant = partitionConfig.getConverter().convert(data, partitionKeyColumnIndex);
                return WriterClassifier.partition(instant, partitionConfig);
            }
            return WriterClassifier.plain(path.getBaseTableName());
        }

        private YTsaurusClient makeYtClient() {
            var clientBuilder = YtUtils.makeYtClientBuilder(path, credentialsConfig);
            if (ytWriterOptions.getProxyRole() != null) {
                clientBuilder.setProxyRole(ytWriterOptions.getProxyRole());
            }
            return clientBuilder.build();
        }
    }

    private static void validateCredentials(OAuthCredentialsConfig credentialsConfig) {
        if (credentialsConfig.getUsername() == null) {
            throw new IllegalArgumentException(
                    "Could not find username to connect to YT as. Please, check your credentials");
        }
        if (credentialsConfig.getToken() == null) {
            throw new IllegalArgumentException(String.format(
                    "Could not find token to connect to YT (username=%s). Please, check your credentials",
                    credentialsConfig.getUsername()));
        }
    }
}
