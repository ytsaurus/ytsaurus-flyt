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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;
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
import tech.ytsaurus.flyt.connectors.ytsaurus.common.providers.reshard.LastPartitionsReshardProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.providers.reshard.ReshardProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;

@Slf4j
public class YtDynamicTableWriterPool implements Serializable, Closeable {
    private static final long serialVersionUID = 1L;

    private static final Duration CACHE_TTL = Duration.ofMinutes(2);
    private static final Duration CACHE_CLEANUP_INTERVAL = Duration.ofMinutes(1);
    // While a writer is busy (buffered rows / in-flight transaction) it must never be evicted, since
    // closing it would drop uncommitted data. Instead of the TTL, a busy writer is given this longer
    // "reprieve", refreshed on every access and on every maintenance pass. It only needs to outlast a
    // single cleanup interval so a still-busy writer is always re-extended before it can expire.
    private static final Duration BUSY_WRITER_REPRIEVE = CACHE_CLEANUP_INTERVAL.multipliedBy(2);

    private final transient Supplier<YTsaurusClient> clientSupplier;
    private final transient Cache<String, YtDynamicTableWriter> cache;
    private final transient ScheduledExecutorService cacheMaintenanceExecutor;

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

    @SuppressWarnings("checkstyle:ParameterNumber")
    public YtDynamicTableWriterPool(@Nullable Cache<String, YtDynamicTableWriter> cache,
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

        // Create and initialize delegate once for the whole pool
        this.dataMetrics = DataMetricsWriterDelegate.create(dataMetricsConfig, dataType);
        this.dataMetrics.open(context);

        this.cacheMaintenanceExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "yt-writer-cache-cleanup");
            thread.setDaemon(true);
            return thread;
        });
        long cleanupPeriodMs = CACHE_CLEANUP_INTERVAL.toMillis();
        this.cacheMaintenanceExecutor.scheduleWithFixedDelay(
                this::cleanupCache, cleanupPeriodMs, cleanupPeriodMs, TimeUnit.MILLISECONDS);
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
    static Cache<String, YtDynamicTableWriter> makeDefaultCache() {
        return makeDefaultCache(CACHE_TTL, Ticker.systemTicker());
    }

    @VisibleForTesting
    static Cache<String, YtDynamicTableWriter> makeDefaultCache(Duration ttl, Ticker ticker) {
        return Caffeine.newBuilder()
                .ticker(ticker)
                .expireAfter(new WriterExpiry(ttl))
                .removalListener((String table, YtDynamicTableWriter writer, RemovalCause cause) -> {
                    if (cause.wasEvicted() && writer != null) {
                        log.info("Evicting writer for table '{}' from cache", table);
                        writer.close();
                    }
                })
                .build();
    }

    @SneakyThrows
    public YtDynamicTableWriter getOrAcquire(WriterClassifier writerClassifier) {
        String tableName = writerClassifier.getTableName();
        YtDynamicTableWriter value = cache.getIfPresent(tableName);
        if (value == null) {
            value = prepareWriter(writerClassifier);
            cache.put(tableName, value);
        }
        return value;
    }

    public Collection<YtDynamicTableWriter> getWriters() {
        return List.copyOf(cache.asMap().values());
    }

    /**
     * Periodic cache maintenance. Re-extends the reprieve of writers that are still busy (so an
     * in-flight transaction can never be evicted regardless of TTL) and then triggers Caffeine's
     * pending eviction so that idle, non-busy writers are closed and dropped.
     */
    @VisibleForTesting
    void cleanupCache() {
        try {
            cache.policy().expireVariably().ifPresent(policy ->
                    cache.asMap().forEach((table, writer) -> {
                        if (writer.isBusy()) {
                            policy.setExpiresAfter(table, BUSY_WRITER_REPRIEVE);
                        }
                    }));
            cache.cleanUp();
        } catch (Exception e) {
            log.error("Unable to finish writer cache cleanup", e);
        }
    }

    public void finish() {
        multipleOperations(YtDynamicTableWriter::finish, "finish");
    }

    @Override
    public void close() {
        cacheMaintenanceExecutor.shutdownNow();
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
        metricsSuppliers.putIfAbsent(tablePath.getFullPath(), new MetricsSupplier(tablePath.getFullPath()));
        MetricsSupplier metricsSupplier = metricsSuppliers.get(tablePath.getFullPath());

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

    /**
     * Access-based expiry (equivalent to {@code expireAfterAccess(ttl)}) that additionally never lets a
     * busy writer expire: while a writer has buffered rows or an in-flight transaction it is granted the
     * longer {@link #BUSY_WRITER_REPRIEVE} instead of the TTL, so it cannot be closed and lose data.
     * The periodic {@link #cleanupCache()} pass keeps re-checking the live busy state for idle writers.
     */
    private static final class WriterExpiry implements Expiry<String, YtDynamicTableWriter> {
        private final long ttlNanos;

        private WriterExpiry(Duration ttl) {
            this.ttlNanos = ttl.toNanos();
        }

        private long expiresInNanos(YtDynamicTableWriter writer) {
            return writer.isBusy() ? BUSY_WRITER_REPRIEVE.toNanos() : ttlNanos;
        }

        @Override
        public long expireAfterCreate(String table, YtDynamicTableWriter writer, long currentTime) {
            return expiresInNanos(writer);
        }

        @Override
        public long expireAfterUpdate(String table, YtDynamicTableWriter writer, long currentTime,
                                      long currentDuration) {
            return expiresInNanos(writer);
        }

        @Override
        public long expireAfterRead(String table, YtDynamicTableWriter writer, long currentTime,
                                    long currentDuration) {
            return expiresInNanos(writer);
        }
    }
}
