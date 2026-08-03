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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
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
    private final transient Set<WriterHandle> handles = ConcurrentHashMap.newKeySet();
    private final transient ConcurrentLinkedQueue<RuntimeException> asynchronousCloseErrors =
            new ConcurrentLinkedQueue<>();
    private final transient Object lifecycleGate = new Object();
    private transient boolean acceptingOperations = true;

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
    public YtDynamicTableWriterPool(@Nullable Cache<String, WriterHandle> cache,
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

    static Cache<String, WriterHandle> makeDefaultCache() {
        return Caffeine.newBuilder()
                .expireAfterAccess(CACHE_TTL)
                // Promptly evict (and close) writers for tables that went silent, without waiting for
                // the next cache access to trigger lazy maintenance.
                .scheduler(Scheduler.systemScheduler())
                .removalListener(retireEvictedWriter())
                .build();
    }

    /**
     * Test-only cache with a controllable {@link Ticker} and {@link Executor}. Passing a same-thread
     * executor ({@code Runnable::run}) makes eviction — and therefore {@link YtDynamicTableWriter#close()}
     * — run synchronously with {@link Cache#cleanUp()}, so tests can assert on committed rows
     * deterministically. No {@link Scheduler} is configured: eviction is driven explicitly via
     * {@code cleanUp()}.
     */
    @VisibleForTesting
    static Cache<String, WriterHandle> makeTestCache(Duration ttl, Ticker ticker, Executor executor) {
        return Caffeine.newBuilder()
                .ticker(ticker)
                .executor(executor)
                .expireAfterAccess(ttl)
                .removalListener(retireEvictedWriter())
                .build();
    }

    /**
     * Retires a writer when its cache entry is removed. Retirement prevents new operations from using
     * that generation; the writer is closed only after its last active lease is released.
     */
    private static RemovalListener<String, WriterHandle> retireEvictedWriter() {
        return (String table, WriterHandle handle, RemovalCause cause) -> {
            if (handle != null) {
                log.info("Removing writer for table '{}' from cache ({})", table, cause);
                handle.retire();
            }
        };
    }

    public void write(WriterClassifier writerClassifier, RowData row) {
        try (WriterLease lease = acquire(writerClassifier)) {
            lease.writer().write(row);
        }
    }

    void ensureWriter(WriterClassifier writerClassifier) {
        try (WriterLease ignored = acquire(writerClassifier)) {
            // Creating the lease eagerly initializes the target table.
        }
    }

    @VisibleForTesting
    WriterLease acquire(WriterClassifier writerClassifier) {
        synchronized (lifecycleGate) {
            checkPoolOpen();
            throwAsynchronousCloseErrorIfAny();
            WriterHandle handle = cache.get(writerClassifier.getTableName(), ignored -> {
                WriterHandle created = new WriterHandle(
                        prepareWriter(writerClassifier), handles::remove, asynchronousCloseErrors::add);
                handles.add(created);
                return created;
            });
            WriterLease lease = handle.tryAcquire();
            if (lease == null) {
                // The entry was retired concurrently with lookup. Remove only that generation and retry.
                cache.asMap().remove(writerClassifier.getTableName(), handle);
                return acquire(writerClassifier);
            }
            return lease;
        }
    }

    @VisibleForTesting
    Collection<YtDynamicTableWriter> getWriters() {
        return cache.asMap().values().stream().map(WriterHandle::writer).collect(Collectors.toUnmodifiableList());
    }

    public void finish() {
        List<WriterLease> leases = new ArrayList<>();
        synchronized (lifecycleGate) {
            checkPoolOpen();
            throwAsynchronousCloseErrorIfAny();
            cache.asMap().values().forEach(handle -> {
                WriterLease lease = handle.tryAcquire();
                if (lease != null) {
                    leases.add(lease);
                }
            });
        }
        try {
            multipleOperations(
                    leases.stream().map(WriterLease::writer).collect(Collectors.toUnmodifiableList()),
                    YtDynamicTableWriter::finish,
                    "finish");
        } finally {
            leases.forEach(WriterLease::close);
        }
    }

    @Override
    public void close() {
        List<WriterHandle> handlesToClose;
        synchronized (lifecycleGate) {
            if (!acceptingOperations) {
                return;
            }
            acceptingOperations = false;
            handlesToClose = List.copyOf(handles);
        }

        try {
            // Retire deterministically before invalidating the cache. Invalidating first would dispatch all
            // blocking writer closures concurrently on Caffeine's executor and make shutdown error handling racy.
            handlesToClose.forEach(WriterHandle::retire);
            cache.invalidateAll();
            cache.cleanUp();
            handlesToClose.forEach(WriterHandle::awaitClosed);
            throwAsynchronousCloseErrorIfAny();
        } finally {
            dataMetrics.close();
        }
    }

    private void multipleOperations(Consumer<YtDynamicTableWriter> operation, String operationName) {
        multipleOperations(getWriters(), operation, operationName);
    }

    private void multipleOperations(Collection<YtDynamicTableWriter> writers,
                                    Consumer<YtDynamicTableWriter> operation,
                                    String operationName) {
        List<Exception> writerExceptions = new ArrayList<>();
        List<String> writerPaths = new ArrayList<>();
        for (YtDynamicTableWriter writer : writers) {
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
        ensureWriter(WriterClassifier.plain(path.getBaseTableName()));
    }

    private void checkPoolOpen() {
        if (!acceptingOperations) {
            throw new IllegalStateException("YT writer pool is closed");
        }
    }

    private void throwAsynchronousCloseErrorIfAny() {
        RuntimeException first = asynchronousCloseErrors.poll();
        if (first == null) {
            return;
        }
        RuntimeException next;
        while ((next = asynchronousCloseErrors.poll()) != null) {
            first.addSuppressed(next);
        }
        throw first;
    }

    static final class WriterHandle {
        private final YtDynamicTableWriter writer;
        private final Consumer<WriterHandle> onClosed;
        private final Consumer<RuntimeException> onCloseError;
        private int leases;
        private boolean retired;
        private boolean closing;
        private boolean closed;

        private WriterHandle(YtDynamicTableWriter writer,
                             Consumer<WriterHandle> onClosed,
                             Consumer<RuntimeException> onCloseError) {
            this.writer = writer;
            this.onClosed = onClosed;
            this.onCloseError = onCloseError;
        }

        private YtDynamicTableWriter writer() {
            return writer;
        }

        private synchronized WriterLease tryAcquire() {
            if (retired) {
                return null;
            }
            leases++;
            return new WriterLease(this);
        }

        private void retire() {
            synchronized (this) {
                retired = true;

                if (leases > 0 || closing) {
                    return;
                }

                closing = true;
            }

            closeWriter();
        }

        private void release() {
            synchronized (this) {
                leases--;

                if (!retired || leases > 0 || closing) {
                    return;
                }

                closing = true;
            }

            closeWriter();
        }

        private void closeWriter() {
            try {
                writer.close();
            } catch (RuntimeException e) {
                onCloseError.accept(e);
            } finally {
                onClosed.accept(this);
                synchronized (this) {
                    closed = true;
                    notifyAll();
                }
            }
        }

        private synchronized void awaitClosed() {
            boolean interrupted = false;
            while (!closed) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static final class WriterLease implements AutoCloseable {
        private WriterHandle handle;

        private WriterLease(WriterHandle handle) {
            this.handle = handle;
        }

        YtDynamicTableWriter writer() {
            return handle.writer();
        }

        @Override
        public void close() {
            WriterHandle current = handle;
            if (current != null) {
                handle = null;
                current.release();
            }
        }
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
}
