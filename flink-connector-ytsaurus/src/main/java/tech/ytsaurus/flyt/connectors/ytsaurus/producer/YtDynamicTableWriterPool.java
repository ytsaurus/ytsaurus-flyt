package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.io.Closeable;
import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
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
import tech.ytsaurus.flyt.connectors.ytsaurus.common.providers.reshard.LastPartitionsReshardProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.providers.reshard.ReshardProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.TemporalCache;

@Slf4j
public class YtDynamicTableWriterPool implements Serializable, Closeable {
    private static final long serialVersionUID = 1L;

    private static final Duration CACHE_TTL = Duration.ofMinutes(2);
    private static final Duration CACHE_CLEANUP_INTERVAL = Duration.ofMinutes(1);

    private final transient Supplier<YTsaurusClient> clientSupplier;
    private final transient WriterCache cache;

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

    // Shared across all writers in this pool
    private final DataMetricsWriterDelegate dataMetrics;

    @VisibleForTesting
    @SuppressWarnings("checkstyle:ParameterNumber")
    YtDynamicTableWriterPool(@Nullable WriterCache cache,
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
        this.metricsSuppliers = new ConcurrentHashMap<>();
        this.tableAttributes = tableAttributes;
        this.retryStrategy = retryStrategy;
        this.reshardingConfig = reshardingConfig;
        this.ytWriterOptions = ytWriterOptions;
        this.locksProvider = locksProvider;

        // Create and initialize delegate once for the whole pool
        this.dataMetrics = DataMetricsWriterDelegate.create(dataMetricsConfig, dataType);
        this.dataMetrics.open(context);

        cache.startCleanup();
    }

    /**
     * @deprecated Custom {@link TemporalCache} injection is retained for compatibility. Use the constructor without
     * a cache parameter.
     */
    @Deprecated
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
        this(cache == null ? makeDefaultCache() : new TemporalCacheAdapter(cache),
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
                dataType,
                dataMetricsConfig);
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
                                    LocksProvider locksProvider,
                                    DataType dataType,
                                    @Nullable DataMetricsConfig dataMetricsConfig) {
        this(makeDefaultCache(),
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
                dataType,
                dataMetricsConfig);
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
        this(clientSupplier,
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

    private static YtDynamicTableWriterCache makeDefaultCache() {
        return new YtDynamicTableWriterCache(CACHE_TTL, CACHE_CLEANUP_INTERVAL);
    }

    public YtDynamicTableWriter getOrAcquire(WriterClassifier writerClassifier) {
        String tableName = writerClassifier.getTableName();
        return cache.getOrAcquire(tableName, () -> prepareWriter(writerClassifier));
    }

    public Collection<YtDynamicTableWriter> getWriters() {
        return cache.valuesSnapshot();
    }

    public void finish() {
        multipleOperations(YtDynamicTableWriter::finish, "finish");
    }

    @Override
    public void close() {
        cache.stopCleanup();
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
            StringBuilder errorDetails = new StringBuilder();
            for (int i = 0; i < writerExceptions.size(); i++) {
                Exception exception = writerExceptions.get(i);
                String writerPath = writerPaths.get(i);
                log.error("Error to {} writer for table at '{}'", operationName, writerPath, exception);

                errorDetails.append(String.format("Writer at '%s': %s", writerPath, exception.getMessage()));
                if (i < writerExceptions.size() - 1) {
                    errorDetails.append("; ");
                }
            }

            RuntimeException exceptionWithDetails = new RuntimeException(
                    String.format("Failure to %s %d writer(-s): %s", operationName, writerExceptions.size(),
                            errorDetails));
            for (Exception e : writerExceptions) {
                exceptionWithDetails.addSuppressed(e);
            }
            throw exceptionWithDetails;
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
        MetricsSupplier metricsSupplier = metricsSuppliers.computeIfAbsent(
                tablePath.getFullPath(), MetricsSupplier::new);

        WriterYtInfo ytInfo = new WriterYtInfo(
                tablePath,
                clientSupplier.get(),
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
                resolveReshardProvider(writerClassifier.getPartitionConfig()),
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
                return new FixedReshardProvider(reshardingConfig);
            case LAST_PARTITIONS:
                return new LastPartitionsReshardProvider(reshardingConfig, partitionConfig);
            default:
                throw new IllegalArgumentException("Unsupported resharding strategy: "
                        + reshardingConfig.getReshardStrategy());
        }
    }

    @SuppressWarnings("deprecation")
    private static final class TemporalCacheAdapter implements WriterCache {
        private final TemporalCache<String, YtDynamicTableWriter> cache;

        private TemporalCacheAdapter(TemporalCache<String, YtDynamicTableWriter> cache) {
            this.cache = cache;
        }

        @Override
        public synchronized YtDynamicTableWriter getOrAcquire(
                String tableName,
                Supplier<YtDynamicTableWriter> writerSupplier) {
            YtDynamicTableWriter writer = cache.get(tableName);
            if (writer == null) {
                writer = Preconditions.checkNotNull(writerSupplier.get());
                cache.put(tableName, writer);
            }
            return writer;
        }

        @Override
        public Collection<YtDynamicTableWriter> valuesSnapshot() {
            return cache.values();
        }

        @Override
        public void startCleanup() {
            cache.schedule();
        }

        @Override
        public void stopCleanup() {
            cache.cancel();
        }
    }
}
