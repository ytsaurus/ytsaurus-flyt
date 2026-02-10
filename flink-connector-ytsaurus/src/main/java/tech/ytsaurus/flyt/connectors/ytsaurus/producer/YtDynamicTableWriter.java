package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.concurrent.RetryStrategy;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.client.request.ReshardTable;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.request.TransactionType;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flyt.locks.api.LockMode;
import tech.ytsaurus.flyt.locks.api.LocksProvider;
import tech.ytsaurus.flyt.locks.api.utils.LocksProviderUtils;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import tech.ytsaurus.flyt.connectors.datametrics.DataMetricsWriterDelegate;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.TrackableField;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtTableAttributes;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtErrorCodes;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.metrics.GaugeLong;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.FutureUtils;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.PartitionScaleUtils;

import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_ENABLE_DYNAMIC_STORE_READ_ATTRIBUTE_NAME;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_EXPIRATION_TIME_ATTRIBUTE_NAME;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_MOUNTED_TABLET_STATE_VALUE;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_SCHEMA_ATTRIBUTE_NAME;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_TABLET_STATE_ATTRIBUTE_NAME;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtErrorCodes.NODE_LOCKED_BY_MOUNT_UNMOUNT;

@Slf4j
public class YtDynamicTableWriter implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final int VALUE_METRIC_CLOSED = -1;

    private static final String LAST_COMMIT_TIMESTAMP_NAME = "lastCommitTimestamp";

    private static final String TRACKED_FIELD_NAME = "trackedField";

    private static final String LAST_TRACKED_FIELD_NAME = "lastTrackedField";

    private static final String SUM_COMMITTED_ROWS_NAME = "sumCommittedRows";

    private static final String SUM_FAILED_ROWS_NAME = "sumFailedRows";

    private static final int TABLE_MOUNT_MAX_ATTEMPTS = 5;
    private static final long TABLE_MOUNT_BACKOFF_MS = 5000;

    public static final long WAIT_MOUNTING_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes
    public static final int WAIT_MOUNTED_BACKOFF_MS = 1000;

    private final RowDataToYtListConverters.RowDataToYtMapConverter ytConverter;

    private final ComplexYtPath path;

    private final YtTableAttributes tableAttributes;

    private final String ysonSchemaString;

    private final TrackableField trackableField;

    private final RetryStrategy retryStrategy;

    private final RetryStrategy locksRetryStrategy;

    private final LocksProvider locksProvider;

    @Nullable
    private final ReshardTable reshardRequest;

    private final YtWriterOptions ytWriterOptions;

    private final transient WriterClassifier writerClassifier;

    private final transient YTsaurusClient client;

    private transient TableSchema schemaToCreate;

    private transient TableSchema schemaToWrite;

    private transient List<CompletableFuture<Void>> transactionDataBuffer;

    private transient List<Map<String, ? extends Serializable>> uncommittedRows;

    private transient List<Map<String, ? extends Serializable>> unflushedRows;

    private transient ApiServiceTransaction currentTransaction;

    private transient ModifyRowsRequest.Builder modificationBuffer;

    private transient Lock commitTransactionLock;

    private transient Lock flushModificationLock;

    private transient ScheduledExecutorService transactionCommitter;

    private transient ScheduledExecutorService modificationFlusher;

    private transient AtomicLong lastTransactionCommit;

    private transient AtomicLong lastModificationFlush;

    private transient AtomicInteger rowsInTransaction;

    private transient AtomicInteger rowsInBuffer;

    private transient AtomicReference<Throwable> error;

    private transient RuntimeContext context;

    private transient MetricGroup ytMetricGroup;

    private transient AtomicLong lastCommitTimestamp;

    private transient AtomicLong lastNonCommittedTrackableField;

    private transient AtomicLong maxCommittedTrackableField;

    private transient AtomicLong lastCommittedTrackableField;

    private transient AtomicLong sumCommittedRows;

    private transient AtomicLong sumFailedRows;

    private final transient MetricsSupplier metricsSupplier;

    private transient String acquiredLock;

    // Shared data metrics delegate (managed by pool, not by individual writers)
    private final DataMetricsWriterDelegate dataMetrics;

    @SuppressWarnings("checkstyle:ParameterNumber")
    public YtDynamicTableWriter(RowDataToYtListConverters.RowDataToYtMapConverter ytConverter,
                                WriterYtInfo ytInfo,
                                TrackableField trackableField,
                                WriterClassifier writerClassifier,
                                RetryStrategy retryStrategy,
                                RetryStrategy locksRetryStrategy,
                                RuntimeContext context,
                                MetricsSupplier metricsSuppliers,
                                YtTableAttributes tableAttributes,
                                @Nullable ReshardTable reshardRequest,
                                YtWriterOptions ytWriterOptions,
                                LocksProvider locksProvider,
                                DataMetricsWriterDelegate dataMetrics) {
        this.ytConverter = ytConverter;
        this.path = ytInfo.getPath();
        this.ysonSchemaString = ytInfo.getYsonSchemaString();
        this.client = ytInfo.getClient();
        this.trackableField = trackableField;
        this.writerClassifier = writerClassifier;
        this.context = context;
        this.metricsSupplier = metricsSuppliers;
        this.tableAttributes = tableAttributes;
        this.retryStrategy = retryStrategy;
        this.locksRetryStrategy = locksRetryStrategy;
        this.reshardRequest = reshardRequest;
        this.ytWriterOptions = ytWriterOptions;
        this.locksProvider = locksProvider;
        this.dataMetrics = dataMetrics;
    }

    @SneakyThrows
    public void open() {
        log.info("Open writer for table: {}", path.getFullPath());

        schemaToCreate = TableSchema.fromYTree(YTreeTextSerializer.deserialize(ysonSchemaString));
        schemaToWrite = schemaToCreate.toWrite();

        createAndMountTableIfNeeded();

        acquireLockForTable(LockMode.SHARED);

        log.info("Lock for write acquired. {}", path.getFullPath());

        transactionDataBuffer = new ArrayList<>();
        uncommittedRows =
                new ArrayList<>(ytWriterOptions.getRowsInTransactionLimit() +
                        ytWriterOptions.getRowsInModificationLimit());
        unflushedRows = new ArrayList<>(ytWriterOptions.getRowsInModificationLimit());
        error = new AtomicReference<>();
        lastTransactionCommit = new AtomicLong(System.currentTimeMillis());
        lastModificationFlush = new AtomicLong(System.currentTimeMillis());
        rowsInTransaction = new AtomicInteger(0);
        rowsInBuffer = new AtomicInteger(0);
        lastCommitTimestamp = new AtomicLong(0);
        lastNonCommittedTrackableField = new AtomicLong(0);
        maxCommittedTrackableField = new AtomicLong(0);
        lastCommittedTrackableField = new AtomicLong(0);
        sumCommittedRows = new AtomicLong(0);
        sumFailedRows = new AtomicLong(0);
        resetModificationBuffer();
        commitTransactionLock = new ReentrantLock();
        flushModificationLock = new ReentrantLock();
        transactionCommitter = Executors.newSingleThreadScheduledExecutor();
        modificationFlusher = Executors.newSingleThreadScheduledExecutor();

        addMetrics();

        transactionCommitter.scheduleAtFixedRate(() -> {
            if (lastTransactionCommit.get() + ytWriterOptions.getCommitTransactionPeriod().toMillis()
                    < System.currentTimeMillis()) {
                commitTransactionLock.lock();
                try {
                    commitTransaction();
                } catch (Exception e) {
                    error.set(e);
                } finally {
                    commitTransactionLock.unlock();
                }
            }
        }, 0L, ytWriterOptions.getCommitTransactionPeriod().toMillis(), TimeUnit.MILLISECONDS);
        modificationFlusher.scheduleAtFixedRate(() -> {
            if (lastModificationFlush.get() + ytWriterOptions.getFlushModificationPeriod().toMillis()
                    < System.currentTimeMillis()) {
                flushModificationLock.lock();
                try {
                    flushModification();
                } catch (Exception e) {
                    error.set(e);
                } finally {
                    flushModificationLock.unlock();
                }
            }
        }, 0L, ytWriterOptions.getFlushModificationPeriod().toMillis(), TimeUnit.MILLISECONDS);
        log.info("YT writer options to {} : {}", path.getFullPath(), ytWriterOptions);
        log.info("YT connection to {} started with schema: {}", path.getFullPath(), schemaToCreate);
    }

    private void addMetrics() {
        ytMetricGroup = context.getMetricGroup()
                .addGroup(path.getClusterName())
                .addGroup(path.getFullPath());

        if (ytMetricGroup instanceof AbstractMetricGroup && ((AbstractMetricGroup<?>) ytMetricGroup).isClosed()) {
            log.error("Metric group is closed. This will lead to stale metric values! Path: {}", path.getFullPath());
            return;
        }

        log.info("Metric group created: {}, {}", path.getFullPath(), ytMetricGroup);
        introduceGauge(SUM_COMMITTED_ROWS_NAME, () -> sumCommittedRows.get());
        introduceGauge(SUM_FAILED_ROWS_NAME, () -> sumFailedRows.get());
        introduceGauge(LAST_COMMIT_TIMESTAMP_NAME, () -> lastCommitTimestamp.get());

        if (trackableField != null) {
            log.info("Field to track: {}", trackableField.getName());
            introduceGauge(TRACKED_FIELD_NAME, () -> maxCommittedTrackableField.get());
            introduceGauge(LAST_TRACKED_FIELD_NAME, () -> lastCommittedTrackableField.get());
        }
    }

    private void introduceGauge(String name, Supplier<Long> gauge) {
        introduceGauge(ytMetricGroup, name, gauge);
    }

    private void introduceGauge(MetricGroup group, String name, Supplier<Long> gauge) {
        group.gauge(name, new GaugeLong(() -> metricsSupplier.getMetric(name).get()));
        metricsSupplier.setMetric(name, gauge);
    }

    public void write(RowData record) {
        dataMetrics.onRecord(record);

        if (modificationSize() == ytWriterOptions.getRowsInModificationLimit()) {
            flushModificationLock.lock();
            try {
                flushModification();
            } finally {
                flushModificationLock.unlock();
            }
        }
        if (rowsInTransaction.get() >= ytWriterOptions.getRowsInTransactionLimit()) {
            commitTransactionLock.lock();
            try {
                if (rowsInTransaction.get() >= ytWriterOptions.getRowsInTransactionLimit()) {
                    commitTransaction();
                }
            } finally {
                commitTransactionLock.unlock();
            }
        }
        flushModificationLock.lock();
        try {
            final Map<String, ? extends Serializable> row = createRow(record);
            modificationBuffer.addInsert(row);
            unflushedRows.add(row);
            rowsInBuffer.incrementAndGet();
        } finally {
            flushModificationLock.unlock();
        }
    }

    public void finish() {
        log.info("Waiting for finish writer for table {}", path.getFullPath());
        flushData();
        log.info("Successful finish writer for table {}", path.getFullPath());
    }

    public void close() {
        log.info("Begin closing writer {}", path.getFullPath());
        try {
            if (transactionCommitter != null) {
                transactionCommitter.shutdown();
                boolean terminated = transactionCommitter
                        .awaitTermination(ytWriterOptions.getTransactionTimeout().toMillis(), TimeUnit.MILLISECONDS);
                if (!terminated) {
                    log.warn("Failed to terminate committer for writer {}", path.getFullPath());
                    transactionCommitter.shutdownNow();
                } else {
                    log.info("Transaction commiter closed successfully for writer {}", path.getFullPath());
                }
            }
            if (modificationFlusher != null) {
                modificationFlusher.shutdown();
                boolean terminated = modificationFlusher
                        .awaitTermination(ytWriterOptions.getTransactionTimeout().toMillis(), TimeUnit.MILLISECONDS);
                if (!terminated) {
                    log.warn("Failed to terminate flusher for writer {}", path.getFullPath());
                    modificationFlusher.shutdownNow();
                } else {
                    log.info("Modification flusher closed successfully for writer {}", path.getFullPath());
                }
            }
            flushData();
            log.info("Data flushed successfully for writer {}", path.getFullPath());
            clearMetrics();
            log.info("Metrics closed successfully for writer {}", path.getFullPath());
            client.close();
            log.info("Writer {} closed successfully", path.getFullPath());

            releaseLock();

        } catch (InterruptedException e) {
            log.error("Writer {} closure interrupted", path.getFullPath(), e);
            transactionCommitter.shutdownNow();
            modificationFlusher.shutdownNow();
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Unexpected failure closing writer {}", path.getFullPath(), e);
            throw e;
        }
    }

    private void clearMetrics() {
        lastCommitTimestamp.set(VALUE_METRIC_CLOSED);
        lastCommittedTrackableField.set(VALUE_METRIC_CLOSED);
        maxCommittedTrackableField.set(VALUE_METRIC_CLOSED);
        log.info("Cleared metrics for path: {}", path.getFullPath());
    }

    public void snapshotState(long checkpointId) {
        log.info("Waiting for commit state {} for table {}", checkpointId, path.getFullPath());
        flushData();
        log.info("Successful commit state {} for table {}", checkpointId, path.getFullPath());
    }

    private void flushData() {
        checkError();
        flushModificationLock.lock();
        commitTransactionLock.lock();
        try {
            flushModification();
            commitTransaction();
        } finally {
            commitTransactionLock.unlock();
            flushModificationLock.unlock();
        }
    }

    public String getPath() {
        return path.getFullPath();
    }

    @SneakyThrows
    @VisibleForTesting
    void createAndMountTableIfNeeded() {
        boolean createdByUs = false;

        Boolean createdByUsNullable = LocksProviderUtils.doWithLockAndPredicate(
                path.getFullPath(),
                LockMode.EXCLUSIVE,
                this::isTableExists,
                this::tryCreateTable,
                locksRetryStrategy,
                locksProvider
        );

        if (createdByUsNullable != null) {
            createdByUs = createdByUsNullable;
        }

        switch (ytWriterOptions.getMountMode()) {
            case ALWAYS:
                mountWithLock();
                break;
            case ON_CREATE:
                if (createdByUs) {
                    mountWithLock();
                } else {
                    waitUntilMounted(WAIT_MOUNTING_TIMEOUT_MS);
                }
                break;
            default:
                throw new IllegalStateException("Mount mode is not supported: " + ytWriterOptions.getMountMode());
        }

    }

    private void acquireLockForTable(LockMode lockMode) {
        String fullTablePath = path.getFullPath();
        log.info("Acquire lock: [{}; {}]", fullTablePath, lockMode);
        acquiredLock = locksProvider.acquireLock(fullTablePath, lockMode);
        log.info("Acquire lock success: [{}; {}; {}]", fullTablePath, lockMode, acquiredLock);
    }

    private void releaseLock() {
        if (acquiredLock != null) {

            String fullTablePath = path.getFullPath();
            log.info("Release lock: [{}; {}]", acquiredLock, fullTablePath);
            locksProvider.releaseLock(acquiredLock);
            log.info("Release exclusive lock success: [{}; {}]", acquiredLock, fullTablePath);
            acquiredLock = null;
        }
    }

    @SneakyThrows
    private void mountWithLock() {
        LocksProviderUtils.doWithLockAndPredicate(
                path.getFullPath(),
                LockMode.EXCLUSIVE,
                this::isTableMounted,
                () -> {
                    mountIfUnmounted();
                    return null;
                },
                locksRetryStrategy,
                locksProvider
        );
    }

    @VisibleForTesting
    boolean isTableMounted() {
        String tabletState = getTableState();
        return tabletState.equals(YT_MOUNTED_TABLET_STATE_VALUE);
    }

    private boolean isTableExists() {
        return client.existsNode(path.getFullPath()).join();
    }

    @SneakyThrows
    void waitUntilMounted(long timeoutMs) {
        log.info("Table {} is created. Not mounting it. Start waiting.", path.getFullPath());
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < timeoutMs) {
            if (YT_MOUNTED_TABLET_STATE_VALUE.equals(getTableState())) {
                log.info("Table {} was successfully mounted by 3rd party.", path.getFullPath());
                return;
            }
            log.info("Table {} is not mounted by 3rd party. Sleeping...", path.getFullPath());
            Thread.sleep(WAIT_MOUNTED_BACKOFF_MS);
        }
        log.warn("Table {} is not mounted by 3rd party. Timeout reached.", path.getFullPath());
    }

    @VisibleForTesting
    boolean tryCreateTable() {
        boolean createdTableByUs = false;
        try {
            Map<String, YTreeNode> attributes = tableAttributes.getAttributes();
            attributes.put(YT_SCHEMA_ATTRIBUTE_NAME, schemaToCreate.toYTree());

            log.info("Create YT table {}. Attributes: {}.", path.getFullPath(), attributes);

            client.createNode(
                            CreateNode.builder()
                                    .setPath(YPath.simple(path.getFullPath()))
                                    .setType(CypressNodeType.TABLE)
                                    .setAttributes(attributes)
                                    .setRecursive(true)
                                    .build())
                    .join();
            createdTableByUs = true;
            log.info("YT table: {} was created successfully.", path.getFullPath());
            applyPostCreateOperations();
        } catch (CompletionException e) {
            YTsaurusError ytsaurusError = unwrapYTSaurusError(e);
            if (!ytsaurusError.matches(YtErrorCodes.NODE_ALREADY_EXISTS)) {
                throw new RuntimeException(String.format("Failure creating table %s", path.getFullPath()), e);
            }
            log.info("Table {} was created by 3rd party. Not an error, continue", path.getFullPath());
        }
        return createdTableByUs;
    }

    private void applyPostCreateOperations() {
        log.info("Apply post create operations for table {}", path.getFullPath());
        applyDynamicStoreRead();
        applyTtl();
        reshardTable();
    }

    private void reshardTable() {
        if (reshardRequest != null) {
            log.info("Reshard table {}", path.getFullPath());
            client.reshardTable(reshardRequest).join();
            log.info("Table {} was resharded successfully.", path.getFullPath());
        }
    }

    @SneakyThrows
    @VisibleForTesting
    void mountIfUnmounted() {
        log.info("Start mounting table {}", path.getFullPath());
        int attempts = 0;
        String tabletState = getTableState();
        while (!tabletState.equals(YT_MOUNTED_TABLET_STATE_VALUE)) {
            if (attempts >= TABLE_MOUNT_MAX_ATTEMPTS) {
                throw new IllegalStateException(String.format(
                        "Failed to mount table %s. Tablet state is %s",
                        path.getFullPath(),
                        tabletState));
            }
            log.info("Tablet state of table {} is {}. Try mount... ({}/{})",
                    path.getFullPath(),
                    tabletState,
                    attempts,
                    TABLE_MOUNT_MAX_ATTEMPTS);
            try {
                client.mountTableAndWaitTablets(MountTable.builder()
                        .setPath(path.getFullPath())
                        .setTimeout(Duration.ofMinutes(1))
                        .build());
                break;
            } catch (CompletionException e) {
                YTsaurusError ytsaurusError = unwrapYTSaurusError(e);
                if (ytsaurusError.matches(NODE_LOCKED_BY_MOUNT_UNMOUNT)) {
                    log.info("Backing off of table {} mounting for {} ms.", path.getFullPath(), TABLE_MOUNT_BACKOFF_MS);
                    attempts++;
                    Thread.sleep(TABLE_MOUNT_BACKOFF_MS);
                    tabletState = getTableState();
                    continue;
                }
                throw new RuntimeException(String.format("Failure mounting table %s", path.getFullPath()), e);
            }
        }
        log.info("Table {} has been mounted", path.getFullPath());
    }

    @VisibleForTesting
    String getTableState() {
        return client
                .getNode(path.getFullPathWithAttribute(YT_TABLET_STATE_ATTRIBUTE_NAME))
                .join()
                .stringValue();
    }

    private void applyTtl() {
        PartitionConfig config = writerClassifier.getPartitionConfig();
        if (config == null) {
            return;
        }
        log.info("Apply TTL for {}. Partition config: {}", path.getFullPath(), config);
        OffsetDateTime current = OffsetDateTime.now(ZoneOffset.UTC);
        List<OffsetDateTime> offsetDateTimes = new ArrayList<>();
        if (config.getPartitionTtlDayCnt() != null) {
            Instant rowDataInstant = writerClassifier.getRowDataInstant();
            OffsetDateTime end = PartitionScaleUtils.getEnd(rowDataInstant, config.getPartitionScale());
            offsetDateTimes.add(end.plus(config.getPartitionTtlDayCnt(), ChronoUnit.DAYS));
        }
        if (config.getPartitionTtlInDaysFromCreation() != null) {
            offsetDateTimes.add(current.plus(config.getPartitionTtlInDaysFromCreation(), ChronoUnit.DAYS));
        }
        if (offsetDateTimes.size() != 0 && config.getPartitionMinTtl() != null) {
            offsetDateTimes.add(current.plus(config.getPartitionMinTtl(), ChronoUnit.DAYS));
        }
        if (offsetDateTimes.size() != 0) {
            log.info("Based on partition config for {} ({}) set TTL = {}",
                    path.getFullPath(),
                    config,
                    Collections.max(offsetDateTimes));
            applyExpirationTime(Collections.max(offsetDateTimes));
        } else {
            log.info("TTL is not being set for {}", path.getFullPath());
        }
    }

    private void applyDynamicStoreRead() {
        log.info("Apply dynamic store read for {}", path.getFullPath());
        client.setNode(path.getFullPathWithAttribute(YT_ENABLE_DYNAMIC_STORE_READ_ATTRIBUTE_NAME),
                YTree.booleanNode(path.isEnableDynamicStoreRead())).join();
    }

    private void applyExpirationTime(OffsetDateTime expireAt) {
        client.setNode(path.getFullPathWithAttribute(YT_EXPIRATION_TIME_ATTRIBUTE_NAME),
                YTree.stringNode(PartitionScaleUtils.formatYt(expireAt))).join();
    }

    private int modificationSize() {
        return rowsInBuffer.get();
    }

    private ApiServiceTransaction createTransaction() {
        return client.startTransaction(
                StartTransaction
                        .builder()
                        .setType(TransactionType.Tablet)
                        .setSticky(true)
                        .setAtomicity(ytWriterOptions.getAtomicity())
                        .build()
        ).join();
    }

    @SneakyThrows
    private void commitTransaction() {
        checkError();
        if (currentTransaction != null && rowsInTransaction.get() != 0) {
            FutureUtils.allOf(transactionDataBuffer).get(ytWriterOptions.getTransactionTimeout().getSeconds(),
                    TimeUnit.SECONDS);
            transactionDataBuffer = new ArrayList<>();

            commitWithRetry();

            final GUID currentTransactionId = currentTransaction.getId();
            currentTransaction = null;
            lastTransactionCommit.set(System.currentTimeMillis());
            uncommittedRows.clear();
            int committedRows = rowsInTransaction.getAndSet(0);
            sumCommittedRows.getAndAdd(committedRows);
            lastCommittedTrackableField = lastNonCommittedTrackableField;
            maxCommittedTrackableField.set(Math.max(
                    maxCommittedTrackableField.get(),
                    lastNonCommittedTrackableField.get()));
            log.info("Commit successful transaction {} with {} rows for table {}",
                    currentTransactionId, committedRows, getPath());
        } else {
            log.info("No data to commit in writer for {}", getPath());
        }
        long current = System.currentTimeMillis();
        if (lastCommitTimestamp.get() == VALUE_METRIC_CLOSED) {
            log.error("Preventing reset of last commit timestamp: was=-1, now={} (we're closed)", current);
            return;
        }
        lastCommitTimestamp.set(current);
    }

    private void commitWithRetry() throws InterruptedException {
        RetryStrategy backoffRetryStrategy = retryStrategy;
        while (backoffRetryStrategy.getNumRemainingRetries() >= 0) {
            try {
                currentTransaction.commit().join();
                onCommitSuccess();
                break;
            } catch (Exception e) {
                log.error("Unable to commit transaction {} for table {}", currentTransaction.getId(), getPath(), e);
                sumFailedRows.getAndAdd(rowsInTransaction.get());
                if (backoffRetryStrategy.getNumRemainingRetries() == 0) {
                    log.error("Unable to retry commit transaction {} for table {}",
                            currentTransaction.getId(), getPath(), e);
                    throw e;
                }
                backoffRetryStrategy = backoffRetryStrategy.getNextRetryStrategy();
                Thread.sleep(backoffRetryStrategy.getRetryDelay().toMillis());

                currentTransaction = createTransaction();
                log.info("Start retry transaction {} for table {}", currentTransaction.getId(), getPath());

                ModifyRowsRequest.Builder modifyRowRequestBuilder = createModifyRowRequestBuilder();
                uncommittedRows.forEach(modifyRowRequestBuilder::addInsert);
                currentTransaction.modifyRows(modifyRowRequestBuilder).join();
            }
        }
    }

    /**
     * Hook called after successful transaction commit.
     * Can be overridden by subclasses to add custom logic.
     */
    protected void onCommitSuccess() {
        dataMetrics.onCommit();
    }

    private void flushModification() {
        commitTransactionLock.lock();
        try {
            if (rowsInBuffer.get() != 0) {
                if (currentTransaction == null) {
                    currentTransaction = createTransaction();
                    log.info("Start transaction {} for table {}", currentTransaction.getId(), getPath());
                }
                CompletableFuture<Void> future = currentTransaction.modifyRows(modificationBuffer);
                transactionDataBuffer.add(future);
                lastModificationFlush.set(System.currentTimeMillis());
                rowsInTransaction.updateAndGet(v -> v + modificationSize());
                uncommittedRows.addAll(unflushedRows);
                unflushedRows.clear();
                resetModificationBuffer();
            }
        } finally {
            commitTransactionLock.unlock();
        }
    }

    private Map<String, ? extends Serializable> createRow(RowData record) {
        if (trackableField != null) {
            lastNonCommittedTrackableField.set(
                    trackableField.getConverter().convert(record, trackableField.getIndex()));
        }
        return (Map<String, ? extends Serializable>) ytConverter.convert(null, record);
    }

    private void resetModificationBuffer() {
        rowsInBuffer.set(0);
        modificationBuffer = createModifyRowRequestBuilder();
    }

    private ModifyRowsRequest.Builder createModifyRowRequestBuilder() {
        return ModifyRowsRequest.builder()
                .setPath(path.getFullPath())
                .setSchema(schemaToWrite);
    }

    private void checkError() {
        Throwable e = this.error.get();
        if (e != null) {
            throw new RuntimeException("Error while writing to YT", e);
        }
    }

    private YTsaurusError unwrapYTSaurusError(CompletionException e) {
        if (!(e.getCause() instanceof YTsaurusError)) {
            throw e;
        }
        return (YTsaurusError) e.getCause();
    }

    public boolean isBusy() {
        return rowsInBuffer.get() != 0 || rowsInTransaction.get() != 0;
    }

    @Override
    public String toString() {
        return "YT Writer at " + path.getFullPath();
    }
}
