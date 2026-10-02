package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.DefaultSerializationResolver;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.Atomicity;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Timeout(10)
class YtBufferedTransactionWriterTest {
    private static final String PATH = "//tmp/prepared_rows";
    private static final TableSchema SCHEMA = TableSchema.builder()
            .addValue("id", ColumnValueType.INT64)
            .addValue("payload", ColumnValueType.STRING)
            .build();
    private static final Duration TRANSACTION_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration COMMIT_PERIOD = Duration.ofSeconds(7);
    private static final Duration FLUSH_PERIOD = Duration.ofSeconds(3);

    private YTsaurusClient client;
    private ApiServiceTransaction transaction;

    @BeforeEach
    void setUp() {
        client = mock(YTsaurusClient.class);
        transaction = newTransaction();
        when(client.startTransaction(any(StartTransaction.class)))
                .thenReturn(CompletableFuture.completedFuture(transaction));
    }

    @Test
    void thresholdsFlushAndCommitBeforePreparingTheNextRow() throws Exception {
        YtBufferedTransactionWriter writer = newWriter(2, 3, noRetry(), () -> { }, () -> { });
        writer.write(() -> row(1));
        writer.write(() -> row(2));
        verify(client, never()).startTransaction(any(StartTransaction.class));

        writer.write(() -> row(3));
        writer.write(() -> row(4));
        assertThat(rowIds(requests(transaction))).containsExactly(1L, 2L);
        verify(transaction, never()).commit();

        writer.write(() -> {
            assertThat(writer.getCommittedRowCount()).isEqualTo(4);
            return row(5);
        });

        assertThat(requests(transaction)).extracting(request -> rowIds(List.of(request)))
                .containsExactly(List.of(1L, 2L), List.of(3L, 4L));
        assertThat(writer.isBusy()).isTrue();
        writer.commit();
        assertThat(writer.getCommittedRowCount()).isEqualTo(4);
        assertThat(writer.isBusy()).isTrue();
        verify(transaction).commit();

        writer.flush();

        assertThat(rowIds(requests(transaction))).containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(writer.getCommittedRowCount()).isEqualTo(5);
        assertThat(writer.getFailedRowCount()).isZero();
        assertThat(writer.isBusy()).isFalse();
        verify(client, times(2)).startTransaction(any(StartTransaction.class));
        verify(transaction, times(2)).commit();
    }

    @Test
    void commitWaitsForEveryAsyncModificationInTheSameTransaction() throws Exception {
        CompletableFuture<Void> firstAck = new CompletableFuture<>();
        CompletableFuture<Void> secondAck = new CompletableFuture<>();
        when(transaction.modifyRows(any(ModifyRowsRequest.Builder.class))).thenReturn(firstAck, secondAck);
        YtBufferedTransactionWriter writer = newWriter(2, 10, noRetry(), () -> { }, () -> { });
        for (Map<String, ?> row : List.of(row(1), row(2), row(3), row(4))) {
            writer.write(() -> row);
        }
        writer.flushModifications();

        verify(client).startTransaction(any(StartTransaction.class));
        verify(transaction, times(2)).modifyRows(any(ModifyRowsRequest.Builder.class));
        assertThat(writer.getCommittedRowCount()).isZero();
        assertThat(writer.isBusy()).isTrue();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch entered = new CountDownLatch(1);
        try {
            Future<Void> committing = executor.submit(() -> {
                entered.countDown();
                writer.commit();
                return null;
            });
            assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
            assertThrows(TimeoutException.class, () -> committing.get(100, TimeUnit.MILLISECONDS));
            firstAck.complete(null);
            assertThrows(TimeoutException.class, () -> committing.get(100, TimeUnit.MILLISECONDS));
            verify(transaction, never()).commit();

            secondAck.complete(null);
            committing.get(1, TimeUnit.SECONDS);
        } finally {
            firstAck.complete(null);
            secondAck.complete(null);
            executor.shutdownNow();
        }

        assertThat(rowIds(requests(transaction))).containsExactly(1L, 2L, 3L, 4L);
        assertThat(writer.getCommittedRowCount()).isEqualTo(4);
        assertThat(writer.getFailedRowCount()).isZero();
        assertThat(writer.isBusy()).isFalse();
        verify(transaction).commit();
    }

    @Test
    void retryReplaysPreparedRowsInOneRequestAndCountsEveryFailedAttempt() throws Exception {
        ApiServiceTransaction firstRetry = newTransaction();
        ApiServiceTransaction secondRetry = newTransaction();
        when(client.startTransaction(any(StartTransaction.class))).thenReturn(
                CompletableFuture.completedFuture(transaction),
                CompletableFuture.completedFuture(firstRetry),
                CompletableFuture.completedFuture(secondRetry));
        when(transaction.commit()).thenReturn(CompletableFuture.failedFuture(new IOException("first failure")));
        when(firstRetry.commit()).thenReturn(CompletableFuture.failedFuture(new IOException("second failure")));
        AtomicInteger conversions = new AtomicInteger();
        AtomicReference<YtBufferedTransactionWriter> currentWriter = new AtomicReference<>();
        List<List<Object>> callbackMetrics = new ArrayList<>();
        YtBufferedTransactionWriter writer = newWriter(2, 10, new FixedRetryStrategy(2, Duration.ZERO),
                () -> callbackMetrics.add(metrics(currentWriter.get())),
                () -> callbackMetrics.add(metrics(currentWriter.get())));
        currentWriter.set(writer);
        for (Map<String, ?> row : List.of(row(1), row(2), row(3), row(4), row(5))) {
            writer.write(() -> {
                conversions.incrementAndGet();
                return row;
            });
        }

        writer.flush();

        assertThat(requests(transaction)).extracting(request -> rowIds(List.of(request)))
                .containsExactly(List.of(1L, 2L), List.of(3L, 4L), List.of(5L));
        for (ApiServiceTransaction retry : List.of(firstRetry, secondRetry)) {
            assertThat(requests(retry)).hasSize(1);
            assertThat(rowIds(requests(retry))).containsExactly(1L, 2L, 3L, 4L, 5L);
            verify(retry).commit();
        }
        assertThat(conversions).hasValue(5);
        assertThat(callbackMetrics).containsExactly(List.of(0L, 10L, true), List.of(5L, 10L, false));
        assertThat(writer.getCommittedRowCount()).isEqualTo(5);
        assertThat(writer.getFailedRowCount()).isEqualTo(10);
        assertThat(writer.getLastCommitTimestamp()).isPositive();
        verify(transaction).commit();
    }

    @Test
    void emptyCommitUpdatesTimestampWithoutInvokingCommitHooks() throws Exception {
        Runnable onCommitSuccess = mock(Runnable.class);
        Runnable onTransactionCommitted = mock(Runnable.class);
        YtBufferedTransactionWriter writer = newWriter(2, 10, noRetry(), onCommitSuccess, onTransactionCommitted);
        long lastTransactionCommit = writer.getLastTransactionCommitTime();
        assertThat(writer.getLastCommitTimestamp()).isZero();

        writer.commit();

        assertThat(writer.getLastCommitTimestamp()).isPositive();
        assertThat(writer.getLastTransactionCommitTime()).isEqualTo(lastTransactionCommit);
        assertThat(metrics(writer)).containsExactly(0L, 0L, false);
        verify(client, never()).startTransaction(any(StartTransaction.class));
        verify(onCommitSuccess, never()).run();
        verify(onTransactionCommitted, never()).run();

        writer.clearMetrics();
        writer.commit();

        assertThat(writer.getLastCommitTimestamp()).isEqualTo(-1);
    }

    @Test
    void metricsRemainReadableWhileCommitRpcIsPending() throws Exception {
        CompletableFuture<Void> commitAck = new CompletableFuture<>();
        CountDownLatch commitRequested = new CountDownLatch(1);
        when(transaction.commit()).thenAnswer(invocation -> {
            commitRequested.countDown();
            return commitAck;
        });
        YtBufferedTransactionWriter writer = newWriter(1, 1, noRetry(), () -> { }, () -> { });
        writer.write(() -> row(1));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<Void> flushing = executor.submit(() -> {
                writer.flush();
                return null;
            });
            assertThat(commitRequested.await(1, TimeUnit.SECONDS)).isTrue();

            Future<List<Object>> reported = executor.submit(() -> List.of(
                    writer.getCommittedRowCount(), writer.getFailedRowCount(),
                    writer.getLastCommitTimestamp(), writer.isBusy()));

            assertThat(reported.get(1, TimeUnit.SECONDS)).containsExactly(0L, 0L, 0L, true);
            assertThat(commitAck.isDone()).isFalse();
            commitAck.complete(null);
            flushing.get(1, TimeUnit.SECONDS);
            assertThat(writer.getCommittedRowCount()).isEqualTo(1);
            assertThat(writer.getLastCommitTimestamp()).isPositive();
            assertThat(writer.isBusy()).isFalse();
        } finally {
            commitAck.complete(null);
            executor.shutdownNow();
        }
    }

    @Test
    void scheduledCallbacksFlushAndCommitWhenDueAndStopGracefully() throws Exception {
        ScheduledExecutorService committer = mock(ScheduledExecutorService.class);
        ScheduledExecutorService flusher = mock(ScheduledExecutorService.class);
        when(committer.awaitTermination(TRANSACTION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).thenReturn(true);
        when(flusher.awaitTermination(TRANSACTION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).thenReturn(true);
        YtBufferedTransactionWriter writer = newWriter(2, 10, noRetry(), () -> { }, () -> { });

        try (MockedStatic<Executors> executors = mockStatic(Executors.class)) {
            executors.when(Executors::newSingleThreadScheduledExecutor).thenReturn(committer, flusher);
            writer.open();
            executors.verify(Executors::newSingleThreadScheduledExecutor, times(2));
            Runnable commitCallback = scheduledCallback(committer, COMMIT_PERIOD);
            Runnable flushCallback = scheduledCallback(flusher, FLUSH_PERIOD);
            writer.write(() -> row(1));

            setTimerTimestamps(writer, Long.MAX_VALUE / 2);
            commitCallback.run();
            flushCallback.run();
            verify(client, never()).startTransaction(any(StartTransaction.class));

            setTimerTimestamps(writer, 0);
            flushCallback.run();
            assertThat(rowIds(requests(transaction))).containsExactly(1L);
            verify(transaction, never()).commit();
            commitCallback.run();
            verify(transaction).commit();
            assertThat(writer.getCommittedRowCount()).isEqualTo(1);
            assertThat(writer.isBusy()).isFalse();
            writer.write(() -> row(2));

            assertThat(writer.closeAsyncTasks()).isEmpty();
            assertThat(writer.isBusy()).isTrue();
            assertThat(writer.getCommittedRowCount()).isEqualTo(1);
            verify(transaction).modifyRows(any(ModifyRowsRequest.Builder.class));
            verify(transaction).commit();

            InOrder shutdown = inOrder(committer, flusher);
            shutdown.verify(committer).shutdown();
            shutdown.verify(committer).awaitTermination(TRANSACTION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            shutdown.verify(flusher).shutdown();
            shutdown.verify(flusher).awaitTermination(TRANSACTION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            verify(committer, never()).shutdownNow();
            verify(flusher, never()).shutdownNow();
            verify(client, never()).close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void scheduledCallbackPreservesFailureForTheNextFlush(boolean failCommit) throws Exception {
        ScheduledExecutorService committer = mock(ScheduledExecutorService.class);
        ScheduledExecutorService flusher = mock(ScheduledExecutorService.class);
        when(committer.awaitTermination(TRANSACTION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).thenReturn(true);
        when(flusher.awaitTermination(TRANSACTION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).thenReturn(true);
        IllegalStateException failure = new IllegalStateException("timer write failed");
        if (failCommit) {
            when(transaction.commit()).thenThrow(failure);
        } else {
            when(transaction.modifyRows(any(ModifyRowsRequest.Builder.class))).thenThrow(failure);
        }
        YtBufferedTransactionWriter writer = newWriter(2, 10, noRetry(), () -> { }, () -> { });

        try (MockedStatic<Executors> executors = mockStatic(Executors.class)) {
            executors.when(Executors::newSingleThreadScheduledExecutor).thenReturn(committer, flusher);
            writer.open();
            Runnable commitCallback = scheduledCallback(committer, COMMIT_PERIOD);
            Runnable flushCallback = scheduledCallback(flusher, FLUSH_PERIOD);
            writer.write(() -> row(1));
            setTimerTimestamps(writer, 0);

            flushCallback.run();
            if (failCommit) {
                commitCallback.run();
            }

            RuntimeException propagated = assertThrows(RuntimeException.class, writer::flush);
            assertThat(propagated.getCause()).isSameAs(failure);
            assertThat(writer.closeAsyncTasks()).isEmpty();
        }
    }

    @Test
    void partialOpenCleanupContinuesAfterInterruptionAndForcesShutdownAfterTimeout() throws Exception {
        ScheduledExecutorService committer = mock(ScheduledExecutorService.class);
        ScheduledExecutorService flusher = mock(ScheduledExecutorService.class);
        IllegalStateException launchFailure = new IllegalStateException("flusher scheduling failed");
        when(flusher.scheduleAtFixedRate(any(Runnable.class), eq(0L),
                eq(FLUSH_PERIOD.toMillis()), eq(TimeUnit.MILLISECONDS))).thenThrow(launchFailure);
        InterruptedException interruption = new InterruptedException("interrupted during shutdown");
        when(committer.awaitTermination(TRANSACTION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS))
                .thenThrow(interruption);
        when(flusher.awaitTermination(TRANSACTION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)).thenReturn(false);
        YtBufferedTransactionWriter writer = newWriter(2, 10, noRetry(), () -> { }, () -> { });

        try (MockedStatic<Executors> executors = mockStatic(Executors.class)) {
            executors.when(Executors::newSingleThreadScheduledExecutor).thenReturn(committer, flusher);
            assertThat(assertThrows(IllegalStateException.class, writer::open)).isSameAs(launchFailure);
            scheduledCallback(committer, COMMIT_PERIOD);
            try {
                assertThat(writer.closeAsyncTasks()).containsExactly(interruption);
                assertThat(Thread.currentThread().isInterrupted()).isTrue();

                InOrder shutdown = inOrder(committer, flusher);
                shutdown.verify(committer).shutdown();
                shutdown.verify(committer).awaitTermination(TRANSACTION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                shutdown.verify(committer).shutdownNow();
                shutdown.verify(flusher).shutdown();
                shutdown.verify(flusher).awaitTermination(TRANSACTION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                shutdown.verify(flusher).shutdownNow();
            } finally {
                Thread.interrupted();
            }
        }
    }

    private YtBufferedTransactionWriter newWriter(int modificationLimit, int transactionLimit,
                                                 RetryStrategy retries, Runnable onCommitSuccess,
                                                 Runnable onTransactionCommitted) {
        return YtBufferedTransactionWriter.builder()
                .client(client)
                .path(PATH)
                .schema(SCHEMA)
                .rowsInModificationLimit(modificationLimit)
                .rowsInTransactionLimit(transactionLimit)
                .transactionTimeout(TRANSACTION_TIMEOUT)
                .commitTransactionPeriod(COMMIT_PERIOD)
                .flushModificationPeriod(FLUSH_PERIOD)
                .atomicity(Atomicity.Full)
                .retryStrategy(retries)
                .onCommitSuccess(onCommitSuccess)
                .onTransactionCommitted(onTransactionCommitted)
                .build();
    }

    private static ApiServiceTransaction newTransaction() {
        ApiServiceTransaction value = mock(ApiServiceTransaction.class);
        when(value.modifyRows(any(ModifyRowsRequest.Builder.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(value.commit()).thenReturn(CompletableFuture.completedFuture(null));
        return value;
    }

    private static Runnable scheduledCallback(ScheduledExecutorService executor, Duration period) {
        ArgumentCaptor<Runnable> callback = ArgumentCaptor.forClass(Runnable.class);
        verify(executor).scheduleAtFixedRate(
                callback.capture(), eq(0L), eq(period.toMillis()), eq(TimeUnit.MILLISECONDS));
        return callback.getValue();
    }

    private static void setTimerTimestamps(YtBufferedTransactionWriter writer, long timestamp)
            throws ReflectiveOperationException {
        for (String fieldName : List.of("lastTransactionCommit", "lastModificationFlush")) {
            Field field = YtBufferedTransactionWriter.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            ((AtomicLong) field.get(writer)).set(timestamp);
        }
    }

    private static List<ModifyRowsRequest> requests(ApiServiceTransaction value) {
        ArgumentCaptor<ModifyRowsRequest.Builder> captor = ArgumentCaptor.forClass(ModifyRowsRequest.Builder.class);
        verify(value, atLeastOnce()).modifyRows(captor.capture());
        return captor.getAllValues().stream().map(ModifyRowsRequest.Builder::build).collect(Collectors.toList());
    }

    private static List<Long> rowIds(List<ModifyRowsRequest> requests) {
        return requests.stream().flatMap(request -> {
            assertThat(request.getPath()).isEqualTo(PATH);
            assertThat(request.getSchema()).isEqualTo(SCHEMA);
            request.convertValues(DefaultSerializationResolver.getInstance());
            return request.getRows().stream().map(row -> row.toYTreeMap(request.getSchema(), false).getLong("id"));
        }).collect(Collectors.toList());
    }

    private static List<Object> metrics(YtBufferedTransactionWriter writer) {
        return List.of(writer.getCommittedRowCount(), writer.getFailedRowCount(), writer.isBusy());
    }

    private static Map<String, ?> row(long id) {
        return Map.of("id", id, "payload", "row-" + id);
    }

    private static RetryStrategy noRetry() {
        return new FixedRetryStrategy(0, Duration.ZERO);
    }
}
