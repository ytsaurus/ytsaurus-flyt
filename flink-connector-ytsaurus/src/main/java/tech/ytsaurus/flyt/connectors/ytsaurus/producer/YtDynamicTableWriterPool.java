package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.io.Closeable;
import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;
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

    private final transient Supplier<YTsaurusClient> clientSupplier;
    private final transient Cache<String, WriterHandle> cache;
    private final transient Set<WriterHandle> writerHandles;
    private final transient ConcurrentLinkedQueue<WriterFailure> backgroundFailures;
    private final transient ReentrantReadWriteLock lifecycleLock;
    private final transient Executor writerCloseExecutor;

    private final transient Map<String, MetricsSupplier> metricsSuppliers;

    private transient boolean closed;

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
        this(CacheSettings.defaults(),
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

    @VisibleForTesting
    @SuppressWarnings("checkstyle:ParameterNumber")
    YtDynamicTableWriterPool(CacheSettings cacheSettings,
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
        this.writerHandles = ConcurrentHashMap.newKeySet();
        this.backgroundFailures = new ConcurrentLinkedQueue<>();
        this.lifecycleLock = new ReentrantReadWriteLock();
        this.writerCloseExecutor = cacheSettings.writerCloseExecutor;
        this.cache = makeCache(cacheSettings);

        // Create and initialize delegate once for the whole pool
        this.dataMetrics = DataMetricsWriterDelegate.create(dataMetricsConfig, dataType);
        this.dataMetrics.open(context);
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

    private Cache<String, WriterHandle> makeCache(CacheSettings settings) {
        return Caffeine.newBuilder()
                .expireAfterAccess(settings.ttl)
                .ticker(settings.ticker)
                .scheduler(settings.scheduler)
                .executor(settings.cacheExecutor)
                .removalListener((String key, WriterHandle handle, RemovalCause cause) -> handle.retire())
                .build();
    }

    public void write(WriterClassifier writerClassifier, RowData row) {
        lifecycleLock.readLock().lock();
        try (WriterLease lease = acquireLocked(writerClassifier)) {
            lease.writer().write(row);
        } finally {
            lifecycleLock.readLock().unlock();
        }
        throwPendingWriterFailures();
    }

    public void snapshotState(long checkpointId) {
        operateAllWriters(writer -> writer.snapshotState(checkpointId), "snapshot state");
    }

    @VisibleForTesting
    @SneakyThrows
    YtDynamicTableWriter getOrAcquire(WriterClassifier writerClassifier) {
        lifecycleLock.readLock().lock();
        try (WriterLease lease = acquireLocked(writerClassifier)) {
            return lease.writer();
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    @VisibleForTesting
    public Collection<YtDynamicTableWriter> getWriters() {
        List<YtDynamicTableWriter> writers = new ArrayList<>();
        for (WriterHandle handle : writerHandles) {
            writers.add(handle.writer);
        }
        return writers;
    }

    public void finish() {
        operateAllWriters(YtDynamicTableWriter::finish, "finish");
    }

    @Override
    public void close() {
        lifecycleLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;

            Set<WriterHandle> handles = new HashSet<>(writerHandles);
            cache.invalidateAll();
            handles.forEach(WriterHandle::retire);
            handles.forEach(WriterHandle::awaitClosed);

            List<WriterFailure> failures = drainWriterFailures();
            try {
                dataMetrics.close();
            } catch (Exception e) {
                failures.add(new WriterFailure("data metrics", e));
            }
            throwIfWriterFailures(failures, "close");
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    private void operateAllWriters(Consumer<YtDynamicTableWriter> operation, String operationName) {
        lifecycleLock.writeLock().lock();
        List<WriterLease> leases = new ArrayList<>();
        List<WriterHandle> handles = new ArrayList<>();
        try {
            ensureOpen();
            throwPendingWriterFailures();
            handles.addAll(writerHandles);
            for (WriterHandle handle : handles) {
                WriterLease lease = handle.tryAcquire();
                if (lease != null) {
                    leases.add(lease);
                }
            }
            multipleOperations(
                    leases.stream().map(WriterLease::writer).collect(java.util.stream.Collectors.toList()),
                    operation,
                    operationName);
        } finally {
            leases.forEach(WriterLease::close);
            handles.forEach(WriterHandle::awaitClosedIfRetired);
            lifecycleLock.writeLock().unlock();
        }
        throwPendingWriterFailures();
    }

    @VisibleForTesting
    private void multipleOperations(Consumer<YtDynamicTableWriter> operation, String operationName) {
        multipleOperations(getWriters(), operation, operationName);
    }

    private void multipleOperations(Collection<YtDynamicTableWriter> writers,
                                    Consumer<YtDynamicTableWriter> operation,
                                    String operationName) {
        List<WriterFailure> failures = new ArrayList<>();
        for (YtDynamicTableWriter writer : writers) {
            try {
                operation.accept(writer);
            } catch (Exception e) {
                failures.add(new WriterFailure(writer.getPath(), e));
            }
        }
        throwIfWriterFailures(failures, operationName);
    }

    private WriterLease acquireLocked(WriterClassifier writerClassifier) {
        ensureOpen();
        throwPendingWriterFailures();
        String tableName = writerClassifier.getTableName();
        while (true) {
            WriterHandle handle = cache.get(tableName, ignored -> createWriterHandle(writerClassifier));
            WriterLease lease = handle.tryAcquire();
            if (lease != null) {
                return lease;
            }
            cache.asMap().remove(tableName, handle);
        }
    }

    private WriterHandle createWriterHandle(WriterClassifier writerClassifier) {
        WriterHandle handle = new WriterHandle(prepareWriter(writerClassifier));
        writerHandles.add(handle);
        return handle;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Writer pool is already closed");
        }
    }

    private void throwPendingWriterFailures() {
        throwIfWriterFailures(drainWriterFailures(), "close evicted");
    }

    private List<WriterFailure> drainWriterFailures() {
        List<WriterFailure> failures = new ArrayList<>();
        WriterFailure failure;
        while ((failure = backgroundFailures.poll()) != null) {
            failures.add(failure);
        }
        return failures;
    }

    private void throwIfWriterFailures(List<WriterFailure> failures, String operationName) {
        if (failures.isEmpty()) {
            return;
        }

        StringBuilder errorDetails = new StringBuilder();
        for (int i = 0; i < failures.size(); i++) {
            WriterFailure failure = failures.get(i);
            log.error("Error to {} writer for table at '{}'", operationName, failure.path, failure.exception);
            errorDetails.append(String.format("Writer at '%s': %s", failure.path, failure.exception.getMessage()));
            if (i < failures.size() - 1) {
                errorDetails.append("; ");
            }
        }

        RuntimeException exceptionWithDetails = new RuntimeException(
                String.format("Failure to %s %d writer(-s): %s", operationName, failures.size(), errorDetails));
        failures.forEach(failure -> exceptionWithDetails.addSuppressed(failure.exception));
        throw exceptionWithDetails;
    }

    @VisibleForTesting
    void cleanUpCache() {
        cache.cleanUp();
    }

    @VisibleForTesting
    long cachedWriterCount() {
        return cache.estimatedSize();
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

    @VisibleForTesting
    static class CacheSettings {
        private final Duration ttl;
        private final Ticker ticker;
        private final Scheduler scheduler;
        private final Executor cacheExecutor;
        private final Executor writerCloseExecutor;

        CacheSettings(Duration ttl,
                      Ticker ticker,
                      Scheduler scheduler,
                      Executor cacheExecutor,
                      Executor writerCloseExecutor) {
            this.ttl = ttl;
            this.ticker = ticker;
            this.scheduler = scheduler;
            this.cacheExecutor = cacheExecutor;
            this.writerCloseExecutor = writerCloseExecutor;
        }

        static CacheSettings defaults() {
            return new CacheSettings(
                    CACHE_TTL,
                    Ticker.systemTicker(),
                    Scheduler.systemScheduler(),
                    ForkJoinPool.commonPool(),
                    ForkJoinPool.commonPool());
        }
    }

    private enum WriterState {
        ACTIVE,
        RETIRED,
        CLOSING,
        CLOSED
    }

    private class WriterHandle {
        private final YtDynamicTableWriter writer;
        private final CompletableFuture<Void> closedFuture = new CompletableFuture<>();

        private WriterState state = WriterState.ACTIVE;
        private int leaseCount;

        private WriterHandle(YtDynamicTableWriter writer) {
            this.writer = writer;
        }

        @Nullable
        private synchronized WriterLease tryAcquire() {
            if (state != WriterState.ACTIVE) {
                return null;
            }
            leaseCount++;
            return new WriterLease(this);
        }

        private void retire() {
            boolean closeNow = false;
            synchronized (this) {
                if (state == WriterState.ACTIVE) {
                    state = WriterState.RETIRED;
                }
                if (state == WriterState.RETIRED && leaseCount == 0) {
                    state = WriterState.CLOSING;
                    closeNow = true;
                }
            }
            if (closeNow) {
                scheduleClose();
            }
        }

        private void release() {
            boolean closeNow = false;
            synchronized (this) {
                if (leaseCount <= 0) {
                    throw new IllegalStateException("Writer lease is already released");
                }
                leaseCount--;
                if (leaseCount == 0 && state == WriterState.RETIRED) {
                    state = WriterState.CLOSING;
                    closeNow = true;
                }
            }
            if (closeNow) {
                scheduleClose();
            }
        }

        private void scheduleClose() {
            try {
                writerCloseExecutor.execute(this::closeWriter);
            } catch (RuntimeException e) {
                completeClose(e);
            }
        }

        private void closeWriter() {
            Exception failure = null;
            try {
                writer.close();
            } catch (Exception e) {
                failure = e;
            }
            completeClose(failure);
        }

        private void completeClose(@Nullable Exception failure) {
            synchronized (this) {
                state = WriterState.CLOSED;
            }
            writerHandles.remove(this);
            if (failure == null) {
                closedFuture.complete(null);
            } else {
                backgroundFailures.add(new WriterFailure(writer.getPath(), failure));
                closedFuture.completeExceptionally(failure);
            }
        }

        private void awaitClosed() {
            try {
                closedFuture.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                backgroundFailures.add(new WriterFailure(
                        writer.getPath(), new RuntimeException("Interrupted while closing writer", e)));
            } catch (ExecutionException e) {
                // The close failure was recorded by closeWriter and will be reported by the pool.
            }
        }

        private void awaitClosedIfRetired() {
            boolean retired;
            synchronized (this) {
                retired = state != WriterState.ACTIVE;
            }
            if (retired) {
                awaitClosed();
            }
        }
    }

    private class WriterLease implements AutoCloseable {
        private final WriterHandle handle;
        private boolean released;

        private WriterLease(WriterHandle handle) {
            this.handle = handle;
        }

        private YtDynamicTableWriter writer() {
            return handle.writer;
        }

        @Override
        public void close() {
            if (!released) {
                released = true;
                handle.release();
            }
        }
    }

    private static class WriterFailure {
        private final String path;
        private final Exception exception;

        private WriterFailure(String path, Exception exception) {
            this.path = path;
            this.exception = exception;
        }
    }
}
