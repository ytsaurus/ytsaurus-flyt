package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.io.Closeable;
import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.ReshardTable;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flyt.connectors.datametrics.DataMetricsConfig;
import tech.ytsaurus.flyt.connectors.datametrics.DataMetricsWriterDelegate;
import tech.ytsaurus.flyt.locks.api.LocksProvider;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardingConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.TrackableField;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtTableAttributes;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.providers.reshard.FixedReshardProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.providers.reshard.LastPartitionReshardProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.providers.reshard.ReshardProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.TemporalCache;

@Slf4j
public class YtDynamicTableWriterPool implements Serializable, Closeable {
    private static final long serialVersionUID = 1L;

    private static final long CACHE_TTL = TimeUnit.MINUTES.toMillis(2);
    private static final long CACHE_CLEANUP_INTERVAL = TimeUnit.MINUTES.toMillis(1);

    private final transient Supplier<YTsaurusClient> clientSupplier;
    private final transient TemporalCache<String, YtDynamicTableWriter> cache;

    private final transient Map<String, MetricsSupplier> metricsSuppliers;

    private final String ysonSchemaString;
    private final ComplexYtPath path;
    private final TrackableField trackableField;
    private final RowDataToYtListConverters.RowDataToYtMapConverter ytConverter;

    private final RuntimeContext context;

    private final YtTableAttributes tableAttributes;

    private final RetryStrategy retryStrategy;

    private final ReshardingConfig reshardingConfig;

    private final YtWriterOptions ytWriterOptions;

    private final LocksProvider locksProvider;

    private final DataType dataType;

    // Shared across all writers in this pool
    private final DataMetricsWriterDelegate dataMetrics;

    @SuppressWarnings("checkstyle:ParameterNumber")
    public YtDynamicTableWriterPool(@Nullable TemporalCache<String, YtDynamicTableWriter> cache,
                                    Supplier<YTsaurusClient> clientSupplier,
                                    RowDataToYtListConverters.RowDataToYtMapConverter ytConverter,
                                    ComplexYtPath path,
                                    String ysonSchemaString,
                                    TrackableField trackableField,
                                    RetryStrategy retryStrategy,
                                    RuntimeContext context,
                                    YtTableAttributes tableAttributes,
                                    ReshardingConfig reshardingConfig,
                                    YtWriterOptions ytWriterOptions,
                                    LocksProvider locksProvider,
                                    DataType dataType,
                                    @Nullable DataMetricsConfig dataMetricsConfig) {
        if (cache == null) {
            cache = makeDefaultCache();
        }
        this.cache = cache;
        this.clientSupplier = clientSupplier;
        this.ysonSchemaString = ysonSchemaString;
        this.path = path;
        this.trackableField = trackableField;
        this.ytConverter = ytConverter;
        this.context = context;
        this.metricsSuppliers = new HashMap<>();
        this.tableAttributes = tableAttributes;
        this.retryStrategy = retryStrategy;
        this.reshardingConfig = reshardingConfig;
        this.ytWriterOptions = ytWriterOptions;
        this.locksProvider = locksProvider;
        this.dataType = dataType;

        // Create and initialize delegate once for the whole pool
        this.dataMetrics = DataMetricsWriterDelegate.create(dataMetricsConfig, dataType);
        this.dataMetrics.open(context);

        cache.schedule();
    }


    @SuppressWarnings("checkstyle:ParameterNumber")
    public YtDynamicTableWriterPool(Supplier<YTsaurusClient> clientSupplier,
                                    RowDataToYtListConverters.RowDataToYtMapConverter ytConverter,
                                    ComplexYtPath path,
                                    String ysonSchemaString,
                                    TrackableField trackableField,
                                    RetryStrategy retryStrategy,
                                    RuntimeContext context,
                                    YtTableAttributes tableAttributes,
                                    ReshardingConfig reshardingConfig,
                                    YtWriterOptions ytWriterOptions,
                                    LocksProvider locksProvider) {
        this(null,
                clientSupplier,
                ytConverter,
                path,
                ysonSchemaString,
                trackableField,
                retryStrategy,
                context,
                tableAttributes,
                reshardingConfig,
                ytWriterOptions,
                locksProvider,
                null,
                null);
    }

    @VisibleForTesting
    static TemporalCache<String, YtDynamicTableWriter> makeDefaultCache() {
        return TemporalCache.<String, YtDynamicTableWriter>builder()
                .ttl(CACHE_TTL, TimeUnit.MILLISECONDS, writer -> !writer.isBusy())
                .cleanupPeriod(CACHE_CLEANUP_INTERVAL, TimeUnit.MILLISECONDS)
                .removalListener(entry -> entry.getValue().close())
                .build();
    }

    @SneakyThrows
    public YtDynamicTableWriter getOrAcquire(WriterClassifier writerClassifier) {
        String tableName = writerClassifier.getTableName();
        YtDynamicTableWriter value = cache.get(tableName);
        if (value == null) {
            value = prepareWriter(writerClassifier);
            cache.put(tableName, value);
        }
        return value;
    }

    public Collection<YtDynamicTableWriter> getWriters() {
        return cache.values();
    }

    public void finish() {
        multipleOperations(YtDynamicTableWriter::finish, "finish");
    }

    @Override
    public void close() {
        cache.cancel();
        multipleOperations(YtDynamicTableWriter::close, "close");
        dataMetrics.close();
    }

    private void multipleOperations(Consumer<YtDynamicTableWriter> operation, String operationName) {
        List<Exception> writerExceptions = new ArrayList<>();
        List<String> writerPaths = new ArrayList<>();
        for (YtDynamicTableWriter writer : getWriters()) {
            try {
                operation.accept(writer);
            } catch (Exception e) {
                writerExceptions.add(e);
                writerPaths.add(writer.getPath());
            }
        }
        if (!writerExceptions.isEmpty()) {
            for (int i = 0; i < writerExceptions.size(); i++) {
                Exception exception = writerExceptions.get(i);
                String writerPath = writerPaths.get(i);
                log.error("Error to {} writer for table at '{}'", operationName, writerPath, exception);
            }
            throw new RuntimeException(String.format("Failure to %s writer(-s). Check logs above.", operationName));
        }
    }

    public void createBasePathMapNode() {
        try (YTsaurusClient client = clientSupplier.get()) {
            boolean mapNodeExists = client.existsNode(path.getBasePath()).join();
            if (!mapNodeExists) {
                client.createNode(
                                CreateNode.builder()
                                        .setPath(YPath.simple(path.getBasePath()))
                                        .setType(CypressNodeType.MAP)
                                        .setRecursive(true)
                                        .build())
                        .join();
            }
        }
    }

    public void createFullPathTable() {
        // Acquiring a table in case of partitioning's absence
        // automatically triggers table init
        getOrAcquire(WriterClassifier.plain(path.getBaseTableName()));
    }


    private YtDynamicTableWriter prepareWriter(WriterClassifier writerClassifier) {
        ComplexYtPath tablePath = path.copy().setTableName(writerClassifier.getTableName());
        metricsSuppliers.putIfAbsent(tablePath.getFullPath(), new MetricsSupplier(tablePath.getFullPath()));
        MetricsSupplier metricsSupplier = metricsSuppliers.get(tablePath.getFullPath());

        YTsaurusClient ytClient = clientSupplier.get();
        ReshardProvider reshardProvider = resolveReshardProvider(writerClassifier.getPartitionConfig());
        ReshardTable reshardRequest = null;
        if (reshardProvider != null) {
            TableSchema schema = TableSchema.fromYTree(YTreeTextSerializer.deserialize(ysonSchemaString));
            reshardRequest = reshardProvider.get(ytClient, tablePath, schema, reshardingConfig);
            if (tableAttributes.getMinTabletCount() == null) {
                // We must add this attribute so that the YT does not compress the number of partitions we set
                log.info("Set min_tablet_count attribute: {}", reshardingConfig.getTabletCount());
                tableAttributes.setMinTabletCount(reshardingConfig.getTabletCount());
            }
        }

        WriterYtInfo ytInfo = new WriterYtInfo(
                tablePath,
                ytClient,
                ysonSchemaString);

        ExponentialBackoffRetryStrategy locksRetryStrategy = new ExponentialBackoffRetryStrategy(
                10,
                Duration.of(1, ChronoUnit.SECONDS),
                Duration.of(60, ChronoUnit.SECONDS)
        );

        YtDynamicTableWriter writer = new YtDynamicTableWriter(
                ytConverter,
                ytInfo,
                trackableField,
                writerClassifier,
                retryStrategy,
                locksRetryStrategy,
                context,
                metricsSupplier,
                tableAttributes,
                reshardRequest,
                ytWriterOptions,
                locksProvider,
                dataMetrics);
        writer.open();
        return writer;
    }

    private ReshardProvider resolveReshardProvider(PartitionConfig partitionConfig) {
        switch (reshardingConfig.getReshardStrategy()) {
            case NONE:
                return null;
            case FIXED:
                return new FixedReshardProvider();
            case LAST_PARTITIONS:
                return new LastPartitionReshardProvider(partitionConfig);
            default:
                throw new IllegalArgumentException("Unsupported resharding strategy: "
                        + reshardingConfig.getReshardStrategy());
        }
    }
}
