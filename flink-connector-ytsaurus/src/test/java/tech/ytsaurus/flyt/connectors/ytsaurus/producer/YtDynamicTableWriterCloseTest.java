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
 * Covers what the writer does, and does not do, with an open transaction on close and on interruption.
 */
class YtDynamicTableWriterCloseTest {
    private static final String SCHEMA =
            "[{\"name\"=\"id\";\"type\"=\"int64\";};{\"name\"=\"date\";\"type\"=\"string\";}]";
    private static final int MODIFICATION_LIMIT = 10;
    private static final RetryStrategy NO_RETRY = new FixedRetryStrategy(0, Duration.ZERO);

    private final Random random = new Random(42);
    private YtDynamicTableWriter writer;

    @AfterEach
    void tearDown() {
        if (writer != null) {
            writer.close();
        }
    }

    @Test
    void close_abortsOpenTransactionWithoutCommitting() {
        RecordingTransactions transactions = new RecordingTransactions(alwaysTrue());
        writer = openWriter(retries(5), transactions);

        // 20 rows are flushed into the transaction, 5 more sit in the modification buffer.
        writeRows(25);
        Assertions.assertEquals(1, transactions.started.size());

        writer.close();

        Assertions.assertTrue(transactions.started.get(0).isAborted(), "open transaction is aborted");
        Assertions.assertEquals(1, transactions.started.size(), "no retry transaction on close");
        Assertions.assertEquals(0, transactions.committedRows.get(), "close never commits");
    }

    @Test
    void close_doesNotThrowWhenCommitWouldFail() {
        RecordingTransactions transactions = new RecordingTransactions(outcomes(false));
        writer = openWriter(NO_RETRY, transactions);

        writeRows(25);

        Assertions.assertDoesNotThrow(() -> writer.close());
        Assertions.assertEquals(0, transactions.committedRows.get());
    }

    @Test
    void interruptedCommit_failsWithoutRetry() {
        RecordingTransactions transactions = new RecordingTransactions(outcomes(false, true));
        writer = openWriter(retries(5), transactions);

        writeRows(25);
        Thread.currentThread().interrupt();
        try {
            Assertions.assertThrows(InterruptedException.class, () -> writer.snapshotState(1));
        } finally {
            Thread.interrupted();
        }

        Assertions.assertEquals(1, transactions.started.size(), "no retry transaction");
        Assertions.assertEquals(0, transactions.committedRows.get());
        Assertions.assertTrue(writer.isBusy(), "rows stay in the transaction");
    }

    private void writeRows(int count) {
        for (int i = 0; i < count; i++) {
            GenericRowData row = new GenericRowData(2);
            row.setField(0, (long) i);
            row.setField(1, TimestampData.fromInstant(Instant.EPOCH));
            writer.write(row);
        }
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

        ComplexYtPath tablePath = ComplexYtPath.builder().basePath("//home/ytsaurus/flink").tableName("tests").build();
        RowType rowType = new RowType(List.of(
                new RowType.RowField("id", new BigIntType()),
                new RowType.RowField("date", new TimestampType())));
        RowDataToYtListConverters.RowDataToYtMapConverter converter = new RowDataToYtListConverters(
                TimestampFormat.ISO_8601).createConverter(rowType, YTreeTextSerializer.deserialize(SCHEMA));
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
                new WriterYtInfo(tablePath,
                        new TestYtClient<>(new BasicEmulatingNodeComponent(), transactions), SCHEMA),
                null,
                WriterClassifier.plain("tests"),
                retryStrategy,
                retries(1),
                context,
                new MetricsSupplier(tablePath.getFullPath()),
                YtTableAttributes.empty(),
                null,
                options,
                new NoopLocksProvider(),
                NoopDataMetricsWriterDelegate.INSTANCE);
        result.open();
        return result;
    }

    /**
     * Starts transactions that follow scripted commit outcomes and record whether they were aborted.
     */
    private final class RecordingTransactions implements TransactionComponent {
        private final List<StubFailingCountingApiServiceTransaction> started =
                Collections.synchronizedList(new ArrayList<>());
        private final AtomicLong committedRows = new AtomicLong();
        private final Iterator<Boolean> commitOutcomes;

        RecordingTransactions(Iterator<Boolean> commitOutcomes) {
            this.commitOutcomes = commitOutcomes;
        }

        @Override
        public CompletableFuture<ApiServiceTransaction> startTransaction(StartTransaction startTransaction) {
            StubFailingCountingApiServiceTransaction transaction = new StubFailingCountingApiServiceTransaction(
                    random, commitOutcomes, committedRows::addAndGet, ignored -> { }, () -> { });
            started.add(transaction);
            return CompletableFuture.completedFuture(transaction);
        }
    }
}
