package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import tech.ytsaurus.flyt.connectors.datametrics.NoopDataMetricsWriterDelegate;
import tech.ytsaurus.flyt.locks.noop.NoopLocksProvider;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtTableAttributes;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;
import tech.ytsaurus.flyt.connectors.ytsaurus.test.TestYtClient;
import tech.ytsaurus.flyt.connectors.ytsaurus.test.component.BasicEmulatingNodeComponent;
import tech.ytsaurus.flyt.connectors.ytsaurus.test.component.StubFailingCountingTransactionComponent;

/**
 * The pool relies on the writer reporting exactly when a commit made it idle.
 */
class YtDynamicTableWriterIdleListenerTest {
    private static final String SCHEMA = "[{\"name\"=\"id\";\"type\"=\"int64\";}]";

    private final AtomicInteger idleReports = new AtomicInteger();
    private StubFailingCountingTransactionComponent transactions;
    private YtDynamicTableWriter writer;

    @BeforeEach
    void setUp() {
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        Mockito.when(context.getMetricGroup()).thenReturn(UnregisteredMetricsGroup.createOperatorMetricGroup());
        transactions = new StubFailingCountingTransactionComponent(
                new Random(42), Iterators.cycle(true), ignored -> { });
        ComplexYtPath tablePath = ComplexYtPath.builder().basePath("//home/ytsaurus/flink").tableName("tests").build();
        RowType rowType = new RowType(List.of(new RowType.RowField("id", new BigIntType())));

        writer = new YtDynamicTableWriter(
                new RowDataToYtListConverters(TimestampFormat.ISO_8601)
                        .createConverter(rowType, YTreeTextSerializer.deserialize(SCHEMA)),
                new WriterYtInfo(tablePath, new TestYtClient<>(new BasicEmulatingNodeComponent(), transactions), SCHEMA),
                null,
                WriterClassifier.plain("tests"),
                new FixedRetryStrategy(0, Duration.ZERO),
                new FixedRetryStrategy(0, Duration.ZERO),
                context,
                new MetricsSupplier(tablePath.getFullPath()),
                YtTableAttributes.empty(),
                null,
                YtWriterOptions.builder()
                        // Keep the background flusher and committer idle so the test drives every commit.
                        .flushModificationPeriod(Duration.ofHours(1))
                        .commitTransactionPeriod(Duration.ofHours(1))
                        .build(),
                new NoopLocksProvider(),
                NoopDataMetricsWriterDelegate.INSTANCE,
                idleReports::incrementAndGet);
        writer.open();
    }

    @AfterEach
    void tearDown() {
        writer.close();
    }

    @Test
    void checkpointReportsIdleOnlyWhenItCommittedRows() {
        writeRows(5);
        Assertions.assertTrue(writer.isBusy());

        writer.snapshotState(1);
        Assertions.assertFalse(writer.isBusy());
        Assertions.assertEquals(5, transactions.getCommittedRows());
        Assertions.assertEquals(1, idleReports.get());

        writer.snapshotState(2);
        Assertions.assertEquals(1, idleReports.get(), "an empty checkpoint is not an idle transition");

        writeRows(3);
        writer.finish();
        Assertions.assertEquals(2, idleReports.get());
    }

    private void writeRows(int count) {
        for (int i = 0; i < count; i++) {
            GenericRowData row = new GenericRowData(1);
            row.setField(0, (long) i);
            writer.write(row);
        }
    }
}
