package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.io.Closeable;
import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.flyt.connectors.datametrics.DataMetricsConfig;
import tech.ytsaurus.flyt.connectors.datametrics.DataMetricsWriterDelegate;
import tech.ytsaurus.flyt.locks.api.LocksProvider;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardingConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.TrackableField;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtTableAttributes;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.providers.reshard.FixedReshardProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.providers.reshard.LastPartitionsReshardProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.providers.reshard.ReshardProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;

/**
 * Keeps one writer per target table and closes writers that stayed idle for {@link #CACHE_TTL}.
 *
 * <p>A write runs inside the cache's compute, so it never races an eviction, and re-evaluates the expiry:
 * a writer holding rows is pinned, an idle one expires. The writer reports when a commit made it idle.
 * Checkpoints and finish iterate the cache without touching expiry.
 */
@Slf4j
public class YtDynamicTableWriterPool implements Serializable, Closeable {
    private static final long serialVersionUID = 1L;

    private static final Duration CACHE_TTL = Duration.ofMinutes(2);
    private static final Duration PINNED = Duration.ofNanos(Long.MAX_VALUE);

    private final transient Supplier<YTsaurusClient> clientSupplier;
    private final transient Cache<String, YtDynamicTableWriter> cache;
    private final transient ScheduledExecutorService cacheExecutor;

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

    // cacheTtl and cacheTicker are test knobs; production builds leave them unset.
    @Builder
    @SuppressWarnings("checkstyle:ParameterNumber")
    private YtDynamicTableWriterPool(Supplier<YTsaurusClient> clientSupplier,
                                     RowDataToYtListConverters.RowDataToYtMapConverter ytConverter,
                                     ComplexYtPath path,
                                     String ysonSchemaString,
                                     @Nullable TrackableField trackableField,
                                     RetryStrategy retryStrategy,
                                     RuntimeContext context,
                                     YtTableAttributes tableAttributes,
                                     ReshardingConfig reshardingConfig,
                                     YtWriterOptions ytWriterOptions,
                                     LocksProvider locksProvider,
                                     @Nullable DataType dataType,
                                     @Nullable DataMetricsConfig dataMetricsConfig,
                                     @Nullable Duration cacheTtl,
                                     @Nullable Ticker cacheTicker) {
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

        this.cacheExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "yt-writer-cache-" + path.getBasePath());
            thread.setDaemon(true);
            return thread;
        });
        this.cache = makeCache(cacheTtl != null ? cacheTtl : CACHE_TTL, cacheTicker);
    }

    private Cache<String, YtDynamicTableWriter> makeCache(Duration ttl, @Nullable Ticker ticker) {
        Caffeine<String, YtDynamicTableWriter> builder = Caffeine.newBuilder()
                // A busy writer never expires; an idle one expires a TTL after the last write or idle report.
                .expireAfter(Expiry.<String, YtDynamicTableWriter>accessing(
                        (tableName, writer) -> writer.isBusy() ? PINNED : ttl))
                .evictionListener(this::closeExpiredWriter);
        if (ticker == null) {
            builder.executor(cacheExecutor)
                    .scheduler(Scheduler.forScheduledExecutorService(cacheExecutor));
        } else {
            // Tests drive time and cleanup by hand, so maintenance runs on the calling thread
            builder.ticker(ticker).executor(Runnable::run);
        }
        return builder.build();
    }

    private void closeExpiredWriter(@Nullable String tableName,
                                    @Nullable YtDynamicTableWriter writer,
                                    RemovalCause cause) {
        if (writer == null) {
            return;
        }
        log.info("Closing writer for table '{}' evicted from the pool ({})", tableName, cause);
        try {
            writer.close();
        } catch (Exception e) {
            log.error("Unable to close writer for table '{}' evicted from the pool", tableName, e);
        }
    }

    public void write(WriterClassifier writerClassifier, RowData record) {
        cache.asMap().compute(writerClassifier.getTableName(), (tableName, cached) -> {
            if (cached != null) {
                cached.write(record);
                return cached;
            }
            YtDynamicTableWriter writer = createWriter(writerClassifier);
            try {
                writer.write(record);
            } catch (Exception e) {
                // A throwing compute creates no mapping, so the new writer must be closed here.
                try {
                    writer.close();
                } catch (Exception closeError) {
                    e.addSuppressed(closeError);
                }
                throw e;
            }
            return writer;
        });
    }

    @VisibleForTesting
    void initializeWriter(WriterClassifier writerClassifier) {
        // Writer creation performs eager table initialization.
        cache.asMap().computeIfAbsent(writerClassifier.getTableName(), ignored -> createWriter(writerClassifier));
    }

    private YtDynamicTableWriter createWriter(WriterClassifier writerClassifier) {
        return prepareWriter(writerClassifier, () -> refreshExpiration(writerClassifier.getTableName()));
    }

    public void finish() {
        multipleOperations(YtDynamicTableWriter::finish, "finish");
    }

    public void snapshotState(long checkpointId) {
        multipleOperations(writer -> writer.snapshotState(checkpointId), "snapshot state");
    }

    @Override
    public void close() {
        try {
            multipleOperations(YtDynamicTableWriter::close, "close");
        } finally {
            // Expired writers are hidden from the iteration above and get closed by the eviction listener.
            cache.invalidateAll();
            cacheExecutor.shutdownNow();
            dataMetrics.close();
        }
    }

    private void multipleOperations(Consumer<YtDynamicTableWriter> operation, String operationName) {
        List<Exception> writerExceptions = new ArrayList<>();
        List<String> writerPaths = new ArrayList<>();
        for (YtDynamicTableWriter writer : List.copyOf(cache.asMap().values())) {
            performOperation(writer, operation, operationName, writerExceptions, writerPaths);
        }
        throwIfOperationsFailed(operationName, writerExceptions, writerPaths);
    }

    private void performOperation(
            YtDynamicTableWriter writer,
            Consumer<YtDynamicTableWriter> operation,
            String operationName,
            List<Exception> writerExceptions,
            List<String> writerPaths) {
        try {
            operation.accept(writer);
        } catch (Exception e) {
            writerExceptions.add(e);
            writerPaths.add(writer.getPath());
            log.error("Error to {} writer for table at '{}'", operationName, writer.getPath(), e);
        }
    }

    private void throwIfOperationsFailed(
            String operationName,
            List<Exception> writerExceptions,
            List<String> writerPaths) {
        if (!writerExceptions.isEmpty()) {
            StringBuilder errorDetails = new StringBuilder();
            for (int i = 0; i < writerExceptions.size(); i++) {
                Exception exception = writerExceptions.get(i);
                String writerPath = writerPaths.get(i);
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
        initializeWriter(WriterClassifier.plain(path.getBaseTableName()));
    }

    @VisibleForTesting
    void cleanUpCache() {
        cache.cleanUp();
    }

    @VisibleForTesting
    int getCachedWritersCount() {
        return cache.asMap().size();
    }

    // Called by the writer after a commit left it idle. A read re-evaluates the expiry
    private void refreshExpiration(String tableName) {
        cache.getIfPresent(tableName);
    }

    @VisibleForTesting
    YtDynamicTableWriter prepareWriter(WriterClassifier writerClassifier, Runnable idleListener) {
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
                dataMetrics,
                idleListener);
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
}
