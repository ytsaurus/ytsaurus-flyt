package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.function.ThrowingRunnable;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.TableWriter;
import tech.ytsaurus.client.YTreeRawTableWriterStub;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.datametrics.DataMetricsConfig;
import tech.ytsaurus.flyt.connectors.datametrics.DataMetricsMetricConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.TrackableField;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.TrackableFieldDataConverter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class YtStaticSinkWriterTest {
    static final String SCHEMA = "[{name=id;type=int64}]";

    @Test
    void emptyWriterDoesNotCreateTableOrUploadEvenOnFlushAndClose() throws Exception {
        Fixture f = new Fixture();
        f.writer.flush(false);
        f.writer.flush(true);
        f.writer.close();
        assertThat(f.clientCalls).isZero();
        verify(f.client, never()).ensureTable();
        verify(f.client, never()).openWriter();
    }

    @Test
    void falseWriteRetriesTheSameDetachedRowOnlyAfterReadiness() throws Exception {
        Fixture f = new Fixture();
        TableWriter<YTreeMapNode> stream = f.addStream(new CompletableFuture<>());
        CompletableFuture<Void> ready = new CompletableFuture<>();
        when(stream.readyEvent()).thenReturn(ready);
        List<Object> attempts = new ArrayList<>();
        CountDownLatch firstAttempt = new CountDownLatch(1);
        when(stream.write(anyList(), any(TableSchema.class))).thenAnswer(call -> {
            attempts.add(call.<List<YTreeMapNode>>getArgument(0).get(0));
            firstAttempt.countDown();
            return attempts.size() > 1;
        });
        CompletableFuture<Void> write = CompletableFuture.runAsync(() -> write(f.writer, 42L));
        assertThat(firstAttempt.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(write).isNotDone();
        ready.complete(null);
        write.get(2, TimeUnit.SECONDS);
        assertThat(attempts).hasSize(2);
        assertThat(attempts.get(0)).isSameAs(attempts.get(1));
        assertThat(f.gauge("pendingRows")).isEqualTo(1L);
        assertThat(f.counter("publishedRows")).isZero();
        f.writer.close();
    }

    @Test
    void trueWriteIsNotRepeatedAndMetricsPublishOnlyAfterClose() throws Exception {
        Fixture f = new Fixture();
        CompletableFuture<Void> close = new CompletableFuture<>();
        TableWriter<YTreeMapNode> stream = f.addStream(close);
        f.writer.write(1L, null);
        verify(stream, times(1)).write(anyList(), any(TableSchema.class));
        f.fireTimer(0);
        assertThat(f.counter("publishedRows")).isZero();
        assertThat(f.gauge("pendingCloses")).isEqualTo(1);
        close.complete(null);
        f.runMailbox();
        assertThat(f.counter("publishedRows")).isEqualTo(1);
        assertThat(f.counter("publishedYsonBytes")).isPositive();
        assertThat(f.gauge("pendingRows")).isEqualTo(0L);
        f.writer.flush(false);
        assertThat(f.counter("publishedRows")).isEqualTo(1);
        f.writer.close();
    }

    @Test
    void initializesRawSdkNameTableBeforeTheFirstRowOfEveryUpload() throws Exception {
        Fixture f = new Fixture();
        YTreeRawTableWriterStub first = new YTreeRawTableWriterStub();
        YTreeRawTableWriterStub second = new YTreeRawTableWriterStub();
        assertThat(first.getSchema().getColumnsCount()).isZero();
        f.opens.add(CompletableFuture.completedFuture(first));
        f.opens.add(CompletableFuture.completedFuture(second));
        f.writer.write(1L, null);
        f.writer.write(2L, null);
        f.writer.flush(false);
        f.writer.write(3L, null);
        f.writer.flush(true);
        assertThat(first.getAcceptedPayloads()).isEqualTo(2);
        assertThat(second.getAcceptedPayloads()).isEqualTo(1);
        assertThat(first.getSchema().getColumnNames()).containsExactly("id");
        assertThat(f.counter("publishedRows")).isEqualTo(3);
        f.writer.close();
    }

    @Test
    void checkpointWaitsBothPreviousClosingAndCurrentActiveUpload() throws Exception {
        Fixture f = new Fixture();
        CompletableFuture<Void> first = new CompletableFuture<>();
        CompletableFuture<Void> second = new CompletableFuture<>();
        f.addStream(first);
        TableWriter<YTreeMapNode> secondWriter = f.addStream(second);
        f.writer.write(1L, null);
        f.fireTimer(0);
        f.writer.write(2L, null);
        CountDownLatch secondCloseStarted = new CountDownLatch(1);
        when(secondWriter.close()).thenAnswer(call -> { secondCloseStarted.countDown(); return second; });
        CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> flush(f.writer));
        assertThat(flush).isNotDone();
        first.complete(null);
        assertThat(secondCloseStarted.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(flush).isNotDone();
        second.complete(null);
        flush.get(2, TimeUnit.SECONDS);
        assertThat(f.counter("publishedRows")).isEqualTo(2);
        f.runMailbox();
        assertThat(f.counter("publishedRows")).isEqualTo(2);
        f.writer.close();
    }

    @Test
    void staleTimerCannotCloseTheNextSession() throws Exception {
        Fixture f = new Fixture();
        f.addStream(CompletableFuture.completedFuture(null));
        TableWriter<YTreeMapNode> second = f.addStream(new CompletableFuture<>());
        f.writer.write(1L, null);
        f.writer.flush(false);
        f.writer.write(2L, null);
        f.fireTimer(0); // Simulate a timer already queued before cancellation.
        verify(second, never()).close();
        f.writer.close();
    }

    @Test
    void asynchronousCloseFailureFailsIdleTaskAndRejectsCheckpoint() throws Exception {
        Fixture f = new Fixture();
        CompletableFuture<Void> close = new CompletableFuture<>();
        f.addStream(close);
        f.writer.write(1L, null);
        f.fireTimer(0);
        close.completeExceptionally(new IOException("commit result unknown"));
        assertThrows(IOException.class, f::runMailbox);
        assertThrows(IOException.class, () -> f.writer.flush(false));
        assertThrows(IOException.class, () -> f.writer.flush(false));
        assertThat(f.counter("publishedRows")).isZero();
        assertThat(f.counter("writeErrors")).isEqualTo(1);
        assertThat(f.counter("sumFailedRows")).isEqualTo(1);
        f.writer.close();
    }

    @Test
    void cleanupCancelsActiveAndClosingWithoutPublishingActive() throws Exception {
        Fixture f = new Fixture();
        CompletableFuture<Void> firstClose = new CompletableFuture<>();
        TableWriter<YTreeMapNode> first = f.addStream(firstClose);
        TableWriter<YTreeMapNode> second = f.addStream(new CompletableFuture<>());
        f.writer.write(1L, null);
        f.fireTimer(0);
        f.writer.write(2L, null);
        f.writer.close();
        f.writer.close();
        verify(first).cancel();
        verify(second).cancel();
        verify(second, never()).close();
        verify(f.client).close();
        firstClose.complete(null);
        f.runMailbox();
        assertThat(f.counter("publishedRows")).isZero();
    }

    @Test
    void hangingCloseHasBoundedWaitAndCannotAcknowledgeCheckpoint() throws Exception {
        Fixture f = new Fixture(Duration.ofMillis(100));
        f.addStream(new CompletableFuture<>());
        f.writer.write(1L, null);
        assertTimeoutPreemptively(Duration.ofSeconds(2), () ->
                assertThrows(IOException.class, () -> f.writer.flush(false)));
        assertThat(f.counter("publishedRows")).isZero();
        f.writer.close();
    }

    @Test
    void openingIsBoundedAndLateWriterIsCancelled() throws Exception {
        Fixture f = new Fixture(Duration.ofMillis(100));
        CompletableFuture<TableWriter<YTreeMapNode>> open = new CompletableFuture<>();
        f.opens.add(open);
        assertTimeoutPreemptively(Duration.ofSeconds(2), () ->
                assertThrows(IOException.class, () -> f.writer.write(1L, null)));
        TableWriter<YTreeMapNode> late = mock(TableWriter.class);
        open.complete(late);
        verify(late).cancel();
        f.writer.close();
    }

    @Test
    void interruptedOpenCancelsWriterEvenWhenItCompletesAfterInterruption() throws Exception {
        Fixture f = new Fixture();
        CompletableFuture<TableWriter<YTreeMapNode>> open = new CompletableFuture<>();
        CountDownLatch openStarted = new CountDownLatch(1);
        doAnswer(call -> { openStarted.countDown(); return open; }).when(f.client).openWriter();
        CompletableFuture<Throwable> result = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try { f.writer.write(1L, null); result.complete(null); }
            catch (Throwable error) { result.complete(error); }
        });
        thread.start();
        assertThat(openStarted.await(2, TimeUnit.SECONDS)).isTrue();
        thread.interrupt();
        assertThat(result.get(2, TimeUnit.SECONDS)).isInstanceOf(InterruptedException.class);
        TableWriter<YTreeMapNode> late = mock(TableWriter.class);
        open.complete(late);
        verify(late).cancel();
        f.writer.close();
        thread.join(2000);
    }

    @Test
    void readinessTimeoutCountsTheRejectedRowOnce() throws Exception {
        Fixture f = new Fixture(Duration.ofMillis(100));
        TableWriter<YTreeMapNode> stream = f.addStream(new CompletableFuture<>());
        when(stream.write(anyList(), any(TableSchema.class))).thenReturn(false);
        when(stream.readyEvent()).thenReturn(new CompletableFuture<>());
        assertThrows(IOException.class, () -> f.writer.write(1L, null));
        assertThrows(IOException.class, () -> f.writer.flush(false));
        assertThat(f.counter("sumFailedRows")).isEqualTo(1);
        f.writer.close();
    }

    @Test
    void failedValidationNeverStartsUpload() throws Exception {
        Fixture f = new Fixture();
        CompletableFuture<Void> invalid = new CompletableFuture<>();
        invalid.completeExceptionally(new IllegalArgumentException("schema mismatch"));
        when(f.client.ensureTable()).thenReturn(invalid);
        assertThrows(IOException.class, () -> f.writer.write(1L, null));
        verify(f.client, never()).openWriter();
        f.writer.close();
    }

    @Test
    void endOfInputPublishesAndRejectsFurtherRecords() throws Exception {
        Fixture f = new Fixture();
        f.addStream(CompletableFuture.completedFuture(null));
        f.writer.write(1L, null);
        f.writer.flush(true);
        assertThat(f.counter("publishedRows")).isEqualTo(1);
        assertThrows(IOException.class, () -> f.writer.write(2L, null));
        f.writer.close();
    }

    @Test
    void dataMetricsAndCtlBelongOnlyToTheSuccessfullyPublishedSession() throws Exception {
        Fixture f = new Fixture();
        RowType type = (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3))).getLogicalType();
        YtStaticSink<RowData> sink = YtStaticSink.forRowData(type,
                        "[{name=id;type=int64};{name=ts;type=timestamp;"
                                + "type_v3={type_name=optional;item=timestamp}}]")
                .setProxy("local").setPath("//tmp/static")
                .setCredentialsProvider(mock(CredentialsProvider.class))
                .setDataMetricsConfig(new DataMetricsConfig("events",
                        List.of(new DataMetricsMetricConfig("ts", "event_ts.max"))))
                .setTrackableField(new TrackableField("ts", 1, type.getTypeAt(1).getTypeRoot(),
                        new TrackableFieldDataConverter(type.getTypeAt(1))))
                .build();
        YtStaticSinkWriter<RowData> writer = f.createWriter(sink);
        CompletableFuture<Void> first = new CompletableFuture<>();
        CompletableFuture<Void> second = new CompletableFuture<>();
        f.addStream(first);
        f.addStream(second);
        writer.write(GenericRowData.of(1L, TimestampData.fromEpochMillis(100)), null);
        f.fireTimer(0);
        writer.write(GenericRowData.of(2L, TimestampData.fromEpochMillis(500)), null);
        assertThat(f.gauge("event_ts.max")).isEqualTo(0L);
        first.complete(null);
        f.runMailbox();
        assertThat(f.gauge("event_ts.max")).isEqualTo(100L);
        assertThat(f.gauge("trackedField")).isEqualTo(100L);
        assertThat(f.gauge("lastTrackedField")).isEqualTo(100L);
        second.completeExceptionally(new IOException("unknown commit"));
        assertThrows(IOException.class, () -> writer.flush(false));
        assertThrows(IOException.class, () -> writer.flush(false));
        assertThat(f.gauge("event_ts.max")).isEqualTo(100L);
        assertThat(f.counter("publishedRows")).isEqualTo(1);
        assertThat(f.counter("sumFailedRows")).isEqualTo(1);
        writer.close();
    }

    private static void write(YtStaticSinkWriter<Long> writer, long value) {
        try { writer.write(value, null); } catch (Exception e) { throw new RuntimeException(e); }
    }

    private static void flush(YtStaticSinkWriter<Long> writer) {
        try { writer.flush(false); } catch (Exception e) { throw new RuntimeException(e); }
    }

    static YtStaticSink.Builder<Long> builder() {
        return YtStaticSink.<Long>builder().setProxy("local").setPath("//tmp/static")
                .setYsonSchema(SCHEMA).setCredentialsProvider(mock(CredentialsProvider.class))
                .setSerializer(value -> YTree.mapBuilder().key("id").value(value).buildMap());
    }

    private static final class Fixture {
        final StaticTableClient client = mock(StaticTableClient.class);
        final Queue<CompletableFuture<TableWriter<YTreeMapNode>>> opens = new ArrayDeque<>();
        final List<ProcessingTimeService.ProcessingTimeCallback> timers = new ArrayList<>();
        final Queue<ThrowingRunnable<? extends Exception>> mailboxActions = new ConcurrentLinkedQueue<>();
        final Map<String, Counter> counters = new HashMap<>();
        final Map<String, Gauge<?>> gauges = new HashMap<>();
        final YtStaticSinkWriter<Long> writer;
        final Sink.InitContext context;
        int clientCalls;

        Fixture() { this(Duration.ofMinutes(5)); }

        Fixture(Duration timeout) {
            context = mock(Sink.InitContext.class);
            ProcessingTimeService time = mock(ProcessingTimeService.class);
            when(time.registerTimer(anyLong(), any())).thenAnswer(call -> {
                timers.add(call.getArgument(1));
                return mock(ScheduledFuture.class);
            });
            when(context.getProcessingTimeService()).thenReturn(time);
            MailboxExecutor mailbox = mock(MailboxExecutor.class);
            doAnswer(call -> { mailboxActions.add(call.getArgument(0)); return null; })
                    .when(mailbox).execute(any(ThrowingRunnable.class), anyString());
            when(context.getMailboxExecutor()).thenReturn(mailbox);
            MetricGroup group = new UnregisteredMetricsGroup() {
                @Override
                public MetricGroup addGroup(String name) { return this; }
                @Override
                public Counter counter(String name) { return counters.computeIfAbsent(name, key -> new SimpleCounter()); }
                @Override
                public <V, G extends Gauge<V>> G gauge(String name, G gauge) { gauges.put(name, gauge); return gauge; }
            };
            SinkWriterMetricGroup sinkGroup = mock(SinkWriterMetricGroup.class);
            when(sinkGroup.addGroup(anyString())).thenReturn(group);
            when(context.metricGroup()).thenReturn(sinkGroup);
            when(client.ensureTable()).thenReturn(CompletableFuture.completedFuture(null));
            when(client.openWriter()).thenAnswer(call -> opens.remove());
            writer = createWriter(builder().setRollInterval(Duration.ofMillis(1))
                    .setWriteTimeout(timeout).build());
        }

        <T> YtStaticSinkWriter<T> createWriter(YtStaticSink<T> sink) {
            return new YtStaticSinkWriter<>(sink, context, () -> { clientCalls++; return client; });
        }

        TableWriter<YTreeMapNode> addStream(CompletableFuture<Void> close) throws Exception {
            TableWriter<YTreeMapNode> stream = mock(TableWriter.class);
            when(stream.write(anyList(), any(TableSchema.class))).thenReturn(true);
            doReturn(close).when(stream).close();
            opens.add(CompletableFuture.completedFuture(stream));
            return stream;
        }

        void fireTimer(int index) throws Exception { timers.get(index).onProcessingTime(1000); }
        void runMailbox() throws Exception {
            while (!mailboxActions.isEmpty()) { mailboxActions.remove().run(); }
        }
        long counter(String name) { return counters.get(name).getCount(); }
        Object gauge(String name) { return gauges.get(name).getValue(); }
    }
}
