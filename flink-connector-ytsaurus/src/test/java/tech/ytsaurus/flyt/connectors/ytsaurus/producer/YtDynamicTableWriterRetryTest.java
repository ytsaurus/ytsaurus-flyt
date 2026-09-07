package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.StubFailingCountingApiServiceTransaction;
import tech.ytsaurus.client.request.AbstractModifyRowsRequest;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.flyt.connectors.datametrics.NoopDataMetricsWriterDelegate;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtTableAttributes;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;
import tech.ytsaurus.flyt.connectors.ytsaurus.test.TestYtClient;
import tech.ytsaurus.flyt.connectors.ytsaurus.test.component.BasicEmulatingNodeComponent;
import tech.ytsaurus.flyt.connectors.ytsaurus.test.component.TransactionComponent;
import tech.ytsaurus.flyt.locks.noop.NoopLocksProvider;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

/**
 * Covers what the writer keeps in memory for commit retries and how it re-sends it.
 */
class YtDynamicTableWriterRetryTest {
    private static final String SCHEMA =
            "[{\"name\"=\"id\";\"type\"=\"int64\";};{\"name\"=\"date\";\"type\"=\"string\";}]";
    private static final int MODIFICATION_LIMIT = 10;
    private static final RetryStrategy NO_RETRY = new FixedRetryStrategy(0, Duration.ZERO);

    private final Random random = new Random(42);
    private YtDynamicTableWriter writer;

    @AfterEach
    void tearDown() {
        if (writer != null) {
            try {
                writer.close();
            } catch (Exception ignored) {
                // a failed commit is expected in some tests; nothing else to release
            }
        }
    }

    @Test
    void noRetry_doesNotRetainRows() {
        RecordingTransactions transactions = new RecordingTransactions(outcomes(false), alwaysTrue());
        writer = openWriter(NO_RETRY, transactions);

        writeRows(25);

        Assertions.assertEquals(0, writer.retainedRowsForRetry());
        Assertions.assertThrows(RuntimeException.class, () -> writer.snapshotState(1));
        Assertions.assertEquals(1, transactions.started.size());
        Assertions.assertEquals(0, transactions.committedRows.get());
    }

    @Test
    void retry_retainsRowsOnlyUntilCommit() {
        RecordingTransactions transactions = new RecordingTransactions(alwaysTrue(), alwaysTrue());
        writer = openWriter(retries(5), transactions);

        writeRows(25);
        Assertions.assertEquals(25, writer.retainedRowsForRetry());

        writer.snapshotState(1);

        Assertions.assertEquals(0, writer.retainedRowsForRetry());
        Assertions.assertEquals(1, transactions.started.size());
        Assertions.assertEquals(25, transactions.committedRows.get());
    }

    @Test
    void retry_resendsRowsInModificationSizedBatches() {
        RecordingTransactions transactions = new RecordingTransactions(outcomes(false, true), alwaysTrue());
        writer = openWriter(retries(5), transactions);

        writeRows(95);
        writer.snapshotState(1);

        List<Integer> expectedBatches = batchesOf(95);
        Assertions.assertEquals(2, transactions.started.size());
        Assertions.assertEquals(expectedBatches, transactions.started.get(0).batchSizes, "normal write path");
        Assertions.assertEquals(expectedBatches, transactions.started.get(1).batchSizes, "retry path");
        Assertions.assertEquals(95, transactions.committedRows.get());
        Assertions.assertEquals(0, writer.retainedRowsForRetry());
    }

    @Test
    void retry_failedResendConsumesRetryAndIsRetried() {
        // 1st transaction: 10 batches accepted, commit fails.
        // 2nd transaction: first re-sent batch fails, so the commit is never attempted.
        // 3rd transaction: everything succeeds.
        Iterator<Boolean> modifyOutcomes = Stream.concat(
                Stream.concat(Collections.nCopies(10, true).stream(), Stream.of(false)),
                Stream.generate(() -> true)).iterator();
        RecordingTransactions transactions = new RecordingTransactions(outcomes(false, true), modifyOutcomes);
        writer = openWriter(retries(2), transactions);

        writeRows(95);
        writer.snapshotState(1);

        Assertions.assertEquals(3, transactions.started.size());
        Assertions.assertEquals(batchesOf(95), transactions.started.get(2).batchSizes);
        Assertions.assertEquals(95, transactions.committedRows.get());
    }

    @Test
    void retry_failedResendExhaustsRetryBudget() {
        Iterator<Boolean> modifyOutcomes = Stream.concat(
                Stream.concat(Collections.nCopies(10, true).stream(), Stream.of(false)),
                Stream.generate(() -> true)).iterator();
        RecordingTransactions transactions = new RecordingTransactions(outcomes(false, true), modifyOutcomes);
        writer = openWriter(retries(1), transactions);

        writeRows(95);

        Assertions.assertThrows(RuntimeException.class, () -> writer.snapshotState(1));
        Assertions.assertEquals(2, transactions.started.size());
        Assertions.assertEquals(0, transactions.committedRows.get());
    }

    private void writeRows(int count) {
        for (int i = 0; i < count; i++) {
            GenericRowData row = new GenericRowData(2);
            row.setField(0, (long) i);
            row.setField(1, TimestampData.fromInstant(Instant.EPOCH));
            writer.write(row);
        }
    }

    private static List<Integer> batchesOf(int rows) {
        List<Integer> batches = new ArrayList<>(Collections.nCopies(rows / MODIFICATION_LIMIT, MODIFICATION_LIMIT));
        if (rows % MODIFICATION_LIMIT != 0) {
            batches.add(rows % MODIFICATION_LIMIT);
        }
        return batches;
    }

    private static RetryStrategy retries(int count) {
        return new ExponentialBackoffRetryStrategy(count, Duration.ZERO, Duration.ZERO);
    }

    private static Iterator<Boolean> outcomes(Boolean... values) {
        return List.of(values).iterator();
    }

    private static Iterator<Boolean> alwaysTrue() {
        return Stream.generate(() -> true).iterator();
    }

    private YtDynamicTableWriter openWriter(RetryStrategy retryStrategy, RecordingTransactions transactions) {
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        Mockito.when(context.getMetricGroup()).thenReturn(UnregisteredMetricsGroup.createOperatorMetricGroup());

        ComplexYtPath path = ComplexYtPath.builder().basePath("//home/ytsaurus/flink").tableName("tests").build();
        RowType rowType = new RowType(List.of(
                new RowType.RowField("id", new BigIntType()),
                new RowType.RowField("date", new TimestampType())));
        RowDataToYtListConverters.RowDataToYtMapConverter converter = new RowDataToYtListConverters(
                TimestampFormat.ISO_8601).createTableRowConverter(rowType, YTreeTextSerializer.deserialize(SCHEMA));
        YtWriterOptions options = YtWriterOptions.builder()
                .rowsInModificationLimit(MODIFICATION_LIMIT)
                .rowsInTransactionLimit(1_000)
                // Keep the background flusher/committer idle so the test drives every flush and commit.
                .flushModificationPeriod(Duration.ofHours(1))
                .commitTransactionPeriod(Duration.ofHours(1))
                .transactionTimeout(Duration.ofSeconds(30))
                .build();

        YtDynamicTableWriter result = new YtDynamicTableWriter(
                converter,
                new WriterYtInfo(path, new TestYtClient<>(new BasicEmulatingNodeComponent(), transactions), SCHEMA),
                null,
                WriterClassifier.plain("tests"),
                retryStrategy,
                retries(1),
                context,
                new MetricsSupplier(path.getFullPath()),
                YtTableAttributes.empty(),
                null,
                options,
                new NoopLocksProvider(),
                NoopDataMetricsWriterDelegate.INSTANCE);
        result.open();
        return result;
    }

    /**
     * Starts transactions that record every modifyRows batch size and follow scripted commit/modify outcomes.
     */
    private final class RecordingTransactions implements TransactionComponent {
        private final List<RecordingTransaction> started = Collections.synchronizedList(new ArrayList<>());
        private final AtomicLong committedRows = new AtomicLong();
        private final Iterator<Boolean> commitOutcomes;
        private final Iterator<Boolean> modifyOutcomes;

        RecordingTransactions(Iterator<Boolean> commitOutcomes, Iterator<Boolean> modifyOutcomes) {
            this.commitOutcomes = commitOutcomes;
            this.modifyOutcomes = modifyOutcomes;
        }

        @Override
        public CompletableFuture<ApiServiceTransaction> startTransaction(StartTransaction startTransaction) {
            RecordingTransaction transaction = new RecordingTransaction();
            started.add(transaction);
            return CompletableFuture.completedFuture(transaction);
        }

        private final class RecordingTransaction extends StubFailingCountingApiServiceTransaction {
            private final List<Integer> batchSizes = Collections.synchronizedList(new ArrayList<>());

            RecordingTransaction() {
                super(random, commitOutcomes, committedRows::addAndGet, ignored -> { }, () -> { });
            }

            @Override
            public CompletableFuture<Void> modifyRows(AbstractModifyRowsRequest.Builder<?, ?> request) {
                batchSizes.add(request.getRowModificationTypes().size());
                if (!modifyOutcomes.next()) {
                    CompletableFuture<Void> failed = new CompletableFuture<>();
                    failed.completeExceptionally(new IllegalStateException("Intended modifyRows failure"));
                    return failed;
                }
                return super.modifyRows(request);
            }
        }
    }
}
