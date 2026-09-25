package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import tech.ytsaurus.flyt.locks.noop.NoopLocksProvider;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardStrategy;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardingConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtTableAttributes;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;

/**
 * Covers when the pool closes a cached writer: only after it stayed idle for the TTL, never while it holds
 * rows, and without the TTL being restarted by checkpoint visits. The mock writer reports the idle
 * transition the way the real one does: once, when a checkpoint commits its rows.
 */
class YtDynamicTableWriterPoolCacheTest {
    private static final Duration TTL = Duration.ofMinutes(2);
    // Caffeine's timer wheel finds an expired entry within about a minute after its deadline.
    private static final Duration TTL_WITH_SLACK = TTL.multipliedBy(2);
    private static final String SCHEMA = "[{\"name\"=\"id\";\"type\"=\"int64\";}]";
    private static final WriterClassifier TABLE = WriterClassifier.plain("table");

    private final AtomicLong nanos = new AtomicLong();
    private YtDynamicTableWriterPool pool;

    @BeforeEach
    void setUp() {
        RuntimeContext context = Mockito.mock(RuntimeContext.class);
        Mockito.when(context.getMetricGroup()).thenReturn(UnregisteredMetricsGroup.createOperatorMetricGroup());
        RowType rowType = new RowType(java.util.List.of(new RowType.RowField("id", new BigIntType())));

        pool = Mockito.spy(YtDynamicTableWriterPool.builder()
                .cacheTtl(TTL)
                .cacheTicker(nanos::get)
                .clientSupplier(() -> null)
                .ytConverter(new RowDataToYtListConverters(TimestampFormat.ISO_8601)
                        .createConverter(rowType, YTreeTextSerializer.deserialize(SCHEMA)))
                .path(ComplexYtPath.builder().basePath("//home/ytsaurus/flink").tableName("tests").build())
                .ysonSchemaString(SCHEMA)
                .retryStrategy(new FixedRetryStrategy(0, Duration.ZERO))
                .context(context)
                .tableAttributes(YtTableAttributes.empty())
                .reshardingConfig(ReshardingConfig.builder().reshardStrategy(ReshardStrategy.NONE).build())
                .ytWriterOptions(YtWriterOptions.builder().build())
                .locksProvider(new NoopLocksProvider())
                .build());
    }

    @AfterEach
    void tearDown() {
        pool.close();
    }

    @Test
    void idleWriterIsClosedAfterTtlAndReplacedOnNextWrite() {
        YtDynamicTableWriter first = installWriter("first").writer;
        pool.initializeWriter(TABLE);
        advance(TTL.minusNanos(1));
        pool.cleanUpCache();
        Mockito.verify(first, Mockito.never()).close();

        advance(TTL_WITH_SLACK);
        pool.cleanUpCache();
        Mockito.verify(first).close();
        Assertions.assertEquals(0, pool.getCachedWritersCount());

        YtDynamicTableWriter second = installWriter("second").writer;
        pool.write(TABLE, row());
        Mockito.verify(second).write(Mockito.any());
        Mockito.verify(first, Mockito.never()).write(Mockito.any());
    }

    @Test
    void busyWriterIsKeptUntilCommitThenExpiresAfterFullTtl() {
        MockWriter mock = installWriter("busy");

        pool.write(TABLE, row());
        advance(TTL.multipliedBy(10));
        pool.cleanUpCache();
        Mockito.verify(mock.writer, Mockito.never()).close();

        // The committer reports the idle transition after a background commit.
        mock.busy.set(false);
        mock.idleListener().run();
        advance(TTL.minusNanos(1));
        pool.cleanUpCache();
        Mockito.verify(mock.writer, Mockito.never()).close();

        advance(TTL_WITH_SLACK);
        pool.cleanUpCache();
        Mockito.verify(mock.writer).close();
    }

    @Test
    void checkpointVisitsDoNotExtendIdleTtl() {
        YtDynamicTableWriter writer = installWriter("idle").writer;
        pool.initializeWriter(TABLE);

        Duration checkpointInterval = TTL.dividedBy(4);
        for (int checkpoint = 1; checkpoint < 4; checkpoint++) {
            advance(checkpointInterval);
            pool.snapshotState(checkpoint);
            pool.cleanUpCache();
            Mockito.verify(writer, Mockito.never()).close();
        }

        // Keep checkpointing past the TTL: the visits must not keep the idle writer alive.
        for (int checkpoint = 4; checkpoint <= 12; checkpoint++) {
            advance(checkpointInterval);
            pool.snapshotState(checkpoint);
            pool.cleanUpCache();
        }
        Mockito.verify(writer).close();
        Assertions.assertEquals(0, pool.getCachedWritersCount());
    }

    @Test
    void checkpointThatCommitsBusyWriterStartsFullTtl() {
        MockWriter mock = installWriter("committed-by-checkpoint");

        pool.write(TABLE, row());
        advance(TTL.multipliedBy(3));
        pool.snapshotState(1);
        pool.cleanUpCache();
        Mockito.verify(mock.writer, Mockito.never()).close();

        advance(TTL_WITH_SLACK);
        pool.cleanUpCache();
        Mockito.verify(mock.writer).close();
    }

    @Test
    void expiredWriterIsClosedBeforeReplacementIsUsed() {
        YtDynamicTableWriter expired = installWriter("expired").writer;
        pool.initializeWriter(TABLE);
        advance(TTL);

        YtDynamicTableWriter replacement = installWriter("replacement").writer;
        pool.write(TABLE, row());

        InOrder inOrder = Mockito.inOrder(expired, replacement);
        inOrder.verify(expired).close();
        inOrder.verify(replacement).write(Mockito.any());
        Assertions.assertEquals(1, pool.getCachedWritersCount());
    }

    @Test
    void closeClosesEveryWriterExactlyOnce() {
        YtDynamicTableWriter stale = installWriter("stale").writer;
        pool.initializeWriter(WriterClassifier.plain("other"));
        YtDynamicTableWriter live = installWriter("live").writer;
        pool.initializeWriter(TABLE);
        advance(TTL);
        // Replaces the expired "table" writer; "other" is expired too but may not be collected yet.
        YtDynamicTableWriter fresh = installWriter("fresh").writer;
        pool.initializeWriter(TABLE);
        Mockito.verify(live).close();

        pool.close();

        Mockito.verify(live, Mockito.times(1)).close();
        Mockito.verify(stale, Mockito.times(1)).close();
        Mockito.verify(fresh, Mockito.times(1)).close();
        Assertions.assertEquals(0, pool.getCachedWritersCount());
    }

    @Test
    void failedFirstWriteClosesTheNewWriterAndRethrows() {
        MockWriter mock = installWriter("failing");
        Mockito.doThrow(new IllegalStateException("boom")).when(mock.writer).write(Mockito.any());

        IllegalStateException error = Assertions.assertThrows(
                IllegalStateException.class, () -> pool.write(TABLE, row()));

        Assertions.assertEquals("boom", error.getMessage());
        Mockito.verify(mock.writer).close();
        Assertions.assertEquals(0, pool.getCachedWritersCount());
    }

    @Test
    void failedWriteOnCachedWriterKeepsItAndRethrows() {
        MockWriter mock = installWriter("failing");
        pool.initializeWriter(TABLE);
        Mockito.doThrow(new IllegalStateException("boom")).when(mock.writer).write(Mockito.any());

        Assertions.assertThrows(IllegalStateException.class, () -> pool.write(TABLE, row()));

        Mockito.verify(mock.writer, Mockito.never()).close();
        Assertions.assertEquals(1, pool.getCachedWritersCount());
        Mockito.verify(pool, Mockito.times(1)).prepareWriter(Mockito.eq(TABLE), Mockito.any());
    }

    private void advance(Duration duration) {
        nanos.addAndGet(duration.toNanos());
    }

    private static GenericRowData row() {
        GenericRowData row = new GenericRowData(1);
        row.setField(0, 1L);
        return row;
    }

    /**
     * Makes writer creation for any table return a fresh mock from now on.
     */
    private MockWriter installWriter(String name) {
        MockWriter mock = new MockWriter(name);
        Mockito.doAnswer(invocation -> {
            mock.idleListener = invocation.getArgument(1);
            return mock.writer;
        }).when(pool).prepareWriter(Mockito.any(), Mockito.any());
        return mock;
    }

    private static final class MockWriter {
        private final YtDynamicTableWriter writer = Mockito.mock(YtDynamicTableWriter.class);
        private final AtomicBoolean busy = new AtomicBoolean();
        private Runnable idleListener;

        private MockWriter(String name) {
            Mockito.when(writer.getPath()).thenReturn(name);
            Mockito.when(writer.isBusy()).thenAnswer(invocation -> busy.get());
            Mockito.doAnswer(invocation -> {
                busy.set(true);
                return null;
            }).when(writer).write(Mockito.any());
            Mockito.doAnswer(invocation -> {
                if (busy.getAndSet(false)) {
                    idleListener().run();
                }
                return null;
            }).when(writer).snapshotState(Mockito.anyLong());
        }

        private Runnable idleListener() {
            Assertions.assertNotNull(idleListener, "writer was not created through the pool");
            return idleListener;
        }
    }
}
