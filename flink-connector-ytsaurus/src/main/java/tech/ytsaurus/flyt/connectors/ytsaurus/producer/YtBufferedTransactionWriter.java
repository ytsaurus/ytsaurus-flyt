package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.concurrent.RetryStrategy;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.Atomicity;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.request.TransactionType;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.FutureUtils;

@Slf4j
public final class YtBufferedTransactionWriter {
    private static final int VALUE_METRIC_CLOSED = -1;

    private static final Duration TRANSACTION_EXPIRATION = Duration.ofSeconds(10);
    private static final Duration TRANSACTION_PING_PERIOD = Duration.ofSeconds(3);

    private final YTsaurusClient client;
    private final String path;
    private final TableSchema schema;
    private final int rowsInModificationLimit;
    private final int rowsInTransactionLimit;
    private final Duration commitTransactionPeriod;
    private final Duration flushModificationPeriod;
    private final Duration transactionTimeout;
    private final Atomicity atomicity;
    private final RetryStrategy retryStrategy;
    private final Runnable onCommitSuccess;
    private final Runnable onTransactionCommitted;

    @Nullable
    private final Runnable commitListener;

    private List<CompletableFuture<Void>> transactionDataBuffer;
    private final List<Map<String, ?>> uncommittedRows;
    private final List<Map<String, ?>> unflushedRows;
    private ApiServiceTransaction currentTransaction;
    private ModifyRowsRequest.Builder modificationBuffer;
    private final Lock commitTransactionLock;
    private final Lock flushModificationLock;
    private ScheduledExecutorService transactionCommitter;
    private ScheduledExecutorService modificationFlusher;
    private final AtomicLong lastTransactionCommit;
    private final AtomicLong lastModificationFlush;
    private final AtomicInteger rowsInTransaction;
    private final AtomicInteger rowsInBuffer;
    private final AtomicReference<Throwable> error;
    private final AtomicLong lastCommitTimestamp;
    private final AtomicLong sumCommittedRows;
    private final AtomicLong sumFailedRows;

    @Builder
    @SuppressWarnings("checkstyle:ParameterNumber")
    private YtBufferedTransactionWriter(
            YTsaurusClient client,
            String path,
            TableSchema schema,
            int rowsInModificationLimit,
            int rowsInTransactionLimit,
            Duration commitTransactionPeriod,
            Duration flushModificationPeriod,
            Duration transactionTimeout,
            Atomicity atomicity,
            RetryStrategy retryStrategy,
            Runnable onCommitSuccess,
            Runnable onTransactionCommitted,
            @Nullable Runnable commitListener) {
        this.client = client;
        this.path = path;
        this.schema = schema;
        this.rowsInModificationLimit = rowsInModificationLimit;
        this.rowsInTransactionLimit = rowsInTransactionLimit;
        this.commitTransactionPeriod = commitTransactionPeriod;
        this.flushModificationPeriod = flushModificationPeriod;
        this.transactionTimeout = transactionTimeout;
        this.atomicity = atomicity;
        this.retryStrategy = retryStrategy;
        this.onCommitSuccess = onCommitSuccess;
        this.onTransactionCommitted = onTransactionCommitted;
        this.commitListener = commitListener;

        transactionDataBuffer = new ArrayList<>();
        uncommittedRows = new ArrayList<>(rowsInTransactionLimit + rowsInModificationLimit);
        unflushedRows = new ArrayList<>(rowsInModificationLimit);
        error = new AtomicReference<>();
        lastTransactionCommit = new AtomicLong(System.currentTimeMillis());
        lastModificationFlush = new AtomicLong(System.currentTimeMillis());
        rowsInTransaction = new AtomicInteger(0);
        rowsInBuffer = new AtomicInteger(0);
        lastCommitTimestamp = new AtomicLong(0);
        sumCommittedRows = new AtomicLong(0);
        sumFailedRows = new AtomicLong(0);
        resetModificationBuffer();
        commitTransactionLock = new ReentrantLock();
        flushModificationLock = new ReentrantLock();
    }

    public void open() {
        transactionCommitter = Executors.newSingleThreadScheduledExecutor();
        modificationFlusher = Executors.newSingleThreadScheduledExecutor();

        transactionCommitter.scheduleAtFixedRate(() -> {
            if (lastTransactionCommit.get() + commitTransactionPeriod.toMillis() < System.currentTimeMillis()) {
                boolean committed = false;
                commitTransactionLock.lock();
                try {
                    committed = commitTransaction();
                } catch (Exception e) {
                    error.compareAndSet(null, e);
                } finally {
                    commitTransactionLock.unlock();
                }
                if (committed) {
                    notifyCommit();
                }
            }
        }, 0L, commitTransactionPeriod.toMillis(), TimeUnit.MILLISECONDS);
        modificationFlusher.scheduleAtFixedRate(() -> {
            if (lastModificationFlush.get() + flushModificationPeriod.toMillis() < System.currentTimeMillis()) {
                flushModificationLock.lock();
                try {
                    flushModification();
                } catch (Exception e) {
                    error.compareAndSet(null, e);
                } finally {
                    flushModificationLock.unlock();
                }
            }
        }, 0L, flushModificationPeriod.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void closeAsyncTasks() {
        // interrupt committer thread, ongoing transaction will be automatically aborted after TRANSACTION_EXPIRATION ttl
        log.info("Close async tasks: {}", path);
        if (transactionCommitter != null) {
            transactionCommitter.shutdownNow();
        }
        if (modificationFlusher != null) {
            modificationFlusher.shutdownNow();
        }
    }

    public void write(Supplier<? extends Map<String, ?>> rowSupplier) {
        if (modificationSize() == rowsInModificationLimit) {
            flushModificationLock.lock();
            try {
                flushModification();
            } finally {
                flushModificationLock.unlock();
            }
        }
        if (rowsInTransaction.get() >= rowsInTransactionLimit) {
            commitTransactionLock.lock();
            try {
                if (rowsInTransaction.get() >= rowsInTransactionLimit) {
                    commitTransaction();
                }
            } finally {
                commitTransactionLock.unlock();
            }
        }
        flushModificationLock.lock();
        try {
            final Map<String, ?> row = rowSupplier.get();
            modificationBuffer.addInsert(row);
            unflushedRows.add(row);
            rowsInBuffer.incrementAndGet();
        } finally {
            flushModificationLock.unlock();
        }
    }

    void flushModifications() {
        flushModificationLock.lock();
        try {
            flushModification();
        } finally {
            flushModificationLock.unlock();
        }
    }

    @VisibleForTesting
    void commit() {
        commitTransactionLock.lock();
        try {
            commitTransaction();
        } finally {
            commitTransactionLock.unlock();
        }
    }

    @SneakyThrows
    public void flush() {
        checkError();
        boolean committed;
        flushModificationLock.lock();
        commitTransactionLock.lock();
        try {
            flushModification();
            committed = commitTransaction();
        } finally {
            commitTransactionLock.unlock();
            flushModificationLock.unlock();
        }
        // An interrupted commit must surface here, or a checkpoint could be acknowledged
        // with the rows still sitting in an open transaction.
        if (!committed && Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Commit interrupted for table " + path);
        }
        if (committed) {
            notifyCommit();
        }
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
                        .setAtomicity(atomicity)
                        .setTransactionTimeout(TRANSACTION_EXPIRATION)
                        .setPingPeriod(TRANSACTION_PING_PERIOD)
                        .build()
        ).join();
    }

    /**
     * Commits the current transaction, retrying on failure.
     *
     * @return {@code true} if data was committed. {@code false} if there was nothing to commit, or the thread
     * was interrupted: then the interrupt flag is restored and the transaction and its rows are left in place.
     */
    @SneakyThrows
    private boolean commitTransaction() {
        checkError();
        boolean committedData = false;
        if (currentTransaction != null && rowsInTransaction.get() != 0) {
            try {
                FutureUtils.allOf(transactionDataBuffer).get(transactionTimeout.getSeconds(), TimeUnit.SECONDS);
                commitWithRetry();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Commit interrupted for table {}, {} rows left uncommitted", path, rowsInTransaction.get());
                return false;
            }
            transactionDataBuffer = new ArrayList<>();

            final GUID currentTransactionId = currentTransaction.getId();
            currentTransaction = null;
            lastTransactionCommit.set(System.currentTimeMillis());
            uncommittedRows.clear();
            int committedRows = rowsInTransaction.getAndSet(0);
            sumCommittedRows.getAndAdd(committedRows);
            onTransactionCommitted.run();
            log.info("Commit successful transaction {} with {} rows for table {}",
                    currentTransactionId, committedRows, path);
            committedData = true;
        } else {
            log.info("No data to commit in writer for {}", path);
        }
        long current = System.currentTimeMillis();
        if (lastCommitTimestamp.get() == VALUE_METRIC_CLOSED) {
            log.error("Preventing reset of last commit timestamp: was=-1, now={} (we're closed)", current);
            return false;
        }
        lastCommitTimestamp.set(current);
        return committedData;
    }

    private void notifyCommit() {
        if (commitListener != null) {
            commitListener.run();
        }
    }

    private void commitWithRetry() throws InterruptedException {
        RetryStrategy backoffRetryStrategy = retryStrategy;
        while (true) {
            try {
                currentTransaction.commit().join();
                onCommitSuccess.run();
                return;
            } catch (Exception e) {
                log.error("Unable to commit transaction {} for table {}", currentTransaction.getId(), path, e);
                sumFailedRows.getAndAdd(rowsInTransaction.get());
                if (backoffRetryStrategy.getNumRemainingRetries() == 0) {
                    log.error("Unable to retry commit transaction {} for table {}",
                            currentTransaction.getId(), path, e);
                    throw e;
                }
                Thread.sleep(backoffRetryStrategy.getRetryDelay().toMillis());
                backoffRetryStrategy = backoffRetryStrategy.getNextRetryStrategy();

                currentTransaction = createTransaction();
                log.info("Start retry transaction {} for table {}", currentTransaction.getId(), path);

                ModifyRowsRequest.Builder modifyRowRequestBuilder = createModifyRowRequestBuilder();
                uncommittedRows.forEach(modifyRowRequestBuilder::addInsert);
                currentTransaction.modifyRows(modifyRowRequestBuilder).join();
            }
        }
    }

    private void flushModification() {
        commitTransactionLock.lock();
        try {
            if (rowsInBuffer.get() != 0) {
                if (currentTransaction == null) {
                    currentTransaction = createTransaction();
                    log.info("Start transaction {} for table {}", currentTransaction.getId(), path);
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

    private void resetModificationBuffer() {
        rowsInBuffer.set(0);
        modificationBuffer = createModifyRowRequestBuilder();
    }

    private ModifyRowsRequest.Builder createModifyRowRequestBuilder() {
        return ModifyRowsRequest.builder()
                .setPath(path)
                .setSchema(schema);
    }

    private void checkError() {
        Throwable e = this.error.get();
        if (e != null) {
            throw new RuntimeException("Error while writing to YT", e);
        }
    }

    public void clearMetrics() {
        lastCommitTimestamp.set(VALUE_METRIC_CLOSED);
    }

    public boolean isBusy() {
        return rowsInBuffer.get() != 0 || rowsInTransaction.get() != 0;
    }

    public long getCommittedRowCount() {
        return sumCommittedRows.get();
    }

    public long getFailedRowCount() {
        return sumFailedRows.get();
    }

    public long getLastCommitTimestamp() {
        return lastCommitTimestamp.get();
    }

    long getLastTransactionCommitTime() {
        return lastTransactionCommit.get();
    }
}
