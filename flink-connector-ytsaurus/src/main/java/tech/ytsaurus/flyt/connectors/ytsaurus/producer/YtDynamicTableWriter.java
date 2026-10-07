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
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.client.request.ReshardTable;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flyt.connectors.datametrics.DataMetricsWriterDelegate;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.TrackableField;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtTableAttributes;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtErrorCodes;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.metrics.GaugeLong;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.providers.reshard.ReshardProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.PartitionScaleUtils;
import tech.ytsaurus.flyt.locks.api.LockMode;
import tech.ytsaurus.flyt.locks.api.LocksProvider;
import tech.ytsaurus.flyt.locks.api.utils.LocksProviderUtils;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

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
    private final ReshardProvider reshardProvider;

    private final YtWriterOptions ytWriterOptions;

    private final transient WriterClassifier writerClassifier;

    private final transient YTsaurusClient client;

    private transient TableSchema schemaToCreate;

    private transient YtBufferedTransactionWriter bufferedWriter;

    private transient RuntimeContext context;

    private transient MetricGroup ytMetricGroup;

    private transient AtomicLong lastNonCommittedTrackableField;

    private transient AtomicLong maxCommittedTrackableField;

    private transient AtomicLong lastCommittedTrackableField;

    private final transient MetricsSupplier metricsSupplier;

    private transient String acquiredLock;

    @Nullable
    private final transient Runnable commitListener;

    private final AtomicBoolean closed = new AtomicBoolean();

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
                                @Nullable ReshardProvider reshardProvider,
                                YtWriterOptions ytWriterOptions,
                                LocksProvider locksProvider,
                                DataMetricsWriterDelegate dataMetrics,
                                @Nullable Runnable commitListener) {
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
        this.reshardProvider = reshardProvider;
        this.ytWriterOptions = ytWriterOptions;
        this.locksProvider = locksProvider;
        this.dataMetrics = dataMetrics;
        this.commitListener = commitListener;
    }


    public void open() {
        try {
            log.info("Open writer for table: {}", path.getFullPath());

            schemaToCreate = TableSchema.fromYTree(YTreeTextSerializer.deserialize(ysonSchemaString));

            createAndMountTableIfNeeded();

            acquireLockForTable(LockMode.SHARED);

            log.info("Lock for write acquired. {}", path.getFullPath());

            lastNonCommittedTrackableField = new AtomicLong(0);
            maxCommittedTrackableField = new AtomicLong(0);
            lastCommittedTrackableField = new AtomicLong(0);
            bufferedWriter = YtBufferedTransactionWriter.builder()
                    .client(client)
                    .path(path.getFullPath())
                    .schema(schemaToCreate.toWrite())
                    .rowsInModificationLimit(ytWriterOptions.getRowsInModificationLimit())
                    .rowsInTransactionLimit(ytWriterOptions.getRowsInTransactionLimit())
                    .commitTransactionPeriod(ytWriterOptions.getCommitTransactionPeriod())
                    .flushModificationPeriod(ytWriterOptions.getFlushModificationPeriod())
                    .transactionTimeout(ytWriterOptions.getTransactionTimeout())
                    .atomicity(ytWriterOptions.getAtomicity())
                    .retryStrategy(retryStrategy)
                    .onCommitSuccess(this::onCommitSuccess)
                    .onTransactionCommitted(() -> {
                        lastCommittedTrackableField.set(lastNonCommittedTrackableField.get());
                        maxCommittedTrackableField.set(Math.max(
                                maxCommittedTrackableField.get(),
                                lastNonCommittedTrackableField.get()));
                    })
                    .commitListener(commitListener)
                    .build();
            addMetrics();

            bufferedWriter.open();
            log.info("YT writer options to {} : {}", path.getFullPath(), ytWriterOptions);
            log.info("YT connection to {} started with schema: {}", path.getFullPath(), schemaToCreate);

        } catch (Exception e) {
            log.error("Error open yt writer: {}", path.getFullPath(), e);

            List<Exception> errorsAsync = closeAsyncTasks();
            errorsAsync.forEach(e::addSuppressed);

            List<Exception> errorsResources = closeResources();
            errorsResources.forEach(e::addSuppressed);

            throw e;
        }
    }

    private List<Exception> closeAsyncTasks() {
        return bufferedWriter == null ? Collections.emptyList() : bufferedWriter.closeAsyncTasks();
    }

    private List<Exception> closeResources() {
        log.info("Close resources for: {}", path.getFullPath());
        List<Exception> errors = new ArrayList<>();

        try {
            if (client != null) {
                client.close();
                log.info("Client closed successfully: {}", path.getFullPath());
            }
        } catch (Exception e) {
            log.error("Error closing client {}", path.getFullPath(), e);
            errors.add(e);
        }

        try {
            releaseLock();
            log.info("Release lock success [{}:{}]", path.getFullPath(), acquiredLock);
        } catch (Exception e) {
            log.error("Error release lock [{}:{}]", path.getFullPath(), acquiredLock, e);
            errors.add(e);
        }

        try {
            clearMetrics();
            log.info("Metrics closed successfully for writer {}", path.getFullPath());
        } catch (Exception e) {
            log.error("Error close metrics for writer {}", path.getFullPath(), e);
            errors.add(e);
        }
        return errors;
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
        introduceGauge(SUM_COMMITTED_ROWS_NAME, () -> bufferedWriter.getCommittedRowCount());
        introduceGauge(SUM_FAILED_ROWS_NAME, () -> bufferedWriter.getFailedRowCount());
        introduceGauge(LAST_COMMIT_TIMESTAMP_NAME, () -> bufferedWriter.getLastCommitTimestamp());

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
        bufferedWriter.write(() -> createRow(record));
    }

    public void finish() {
        log.info("Waiting for finish writer for table {}", path.getFullPath());
        flushData();
        log.info("Successful finish writer for table {}", path.getFullPath());
    }

    /**
     * Releases resources only; nothing is flushed or committed here.
     * Durability belongs to {@link #finish()} (graceful stop) and {@link #snapshotState(long)} (checkpoints):
     * Flink also calls close on failure and cancel, where everything past the last checkpoint is replayed anyway.
     * Never throws and is bounded in time, so a stuck YT cannot turn a close into a TaskManager kill.
     */
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        log.info("Begin closing writer {}", path.getFullPath());
        closeAsyncTasks();
        if (bufferedWriter != null) {
            bufferedWriter.abortCurrentTransaction();
        }
        closeResources();
        log.info("Writer {} closed", path.getFullPath());
    }

    private void clearMetrics() {
        bufferedWriter.clearMetrics();
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
        bufferedWriter.flush();
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
                this::tryCreateAndConfigureTheTable,
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
    boolean tryCreateAndConfigureTheTable() {
        boolean createdTableByUs = false;
        try {
            configureTableAttributes();
            createTable();
            createdTableByUs = true;
            log.info("YT table: {} was created successfully.", path.getFullPath());
            applyDynamicStoreRead();
            applyTtl();
            reshardTable();
        } catch (CompletionException e) {
            YTsaurusError ytsaurusError = unwrapYTSaurusError(e);
            if (!ytsaurusError.matches(YtErrorCodes.NODE_ALREADY_EXISTS)) {
                throw new RuntimeException(String.format("Failure creating table %s", path.getFullPath()), e);
            }
            log.info("Table {} was created by 3rd party. Not an error, continue", path.getFullPath());
        }
        return createdTableByUs;
    }

    private void configureTableAttributes() {
        if (reshardProvider != null) {
            int tabletCount = reshardProvider.calculateTabletCount(client, path);
            // We must add this attribute so that the YT does not compress the number of partitions we set
            log.info("Set min_tablet_count attribute: {}", tabletCount);
            tableAttributes.setMinTabletCount(tabletCount);
        }
    }

    private void createTable() {
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
    }

    private void reshardTable() {
        Integer tabletCount = tableAttributes.getMinTabletCount();
        if (reshardProvider != null && tabletCount != null) {
            log.info("Preparing reshard request for YT table '{}' to '{}' tablets with reshard config {}",
                    path.getFullPath(), tabletCount, reshardProvider.getReshardingConfig());
            ReshardTable reshardRequest = reshardProvider.makeReshardRequest(path, schemaToCreate, tabletCount);
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
                        .build())
                        .join();
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

    /**
     * Hook called after successful transaction commit.
     * Can be overridden by subclasses to add custom logic.
     */
    protected void onCommitSuccess() {
        dataMetrics.onCommit();
    }

    private Map<String, ? extends Serializable> createRow(RowData record) {
        if (trackableField != null) {
            lastNonCommittedTrackableField.set(
                    trackableField.getConverter().convert(record, trackableField.getIndex()));
        }
        return (Map<String, ? extends Serializable>) ytConverter.convert(null, record);
    }

    private YTsaurusError unwrapYTSaurusError(CompletionException e) {
        if (!(e.getCause() instanceof YTsaurusError)) {
            throw e;
        }
        return (YTsaurusError) e.getCause();
    }

    public boolean isBusy() {
        return bufferedWriter.isBusy();
    }

    @Override
    public String toString() {
        return "YT Writer at " + path.getFullPath();
    }
}
