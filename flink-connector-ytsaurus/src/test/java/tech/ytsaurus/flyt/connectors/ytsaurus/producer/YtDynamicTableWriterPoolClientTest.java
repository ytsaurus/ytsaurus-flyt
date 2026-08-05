package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Ticker;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import tech.ytsaurus.flyt.locks.noop.NoopLocksProvider;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardStrategy;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardingConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtTableAttributes;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionScale;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.YtPartitioningInstantRowDataConverter;
import tech.ytsaurus.flyt.connectors.ytsaurus.test.CountingTestYtClientPool;
import tech.ytsaurus.flyt.connectors.ytsaurus.test.RandomWithFlipFalseBooleanIterator;
import tech.ytsaurus.flyt.connectors.ytsaurus.test.TestYtClient;
import tech.ytsaurus.flyt.connectors.ytsaurus.test.YtClientPool;
import tech.ytsaurus.flyt.connectors.ytsaurus.test.component.BasicEmulatingNodeComponent;
import tech.ytsaurus.flyt.connectors.ytsaurus.test.component.StubFailingCountingTransactionComponent;

@Slf4j
// Enable logging if in need to investigate.
// @ExtendWith(EnableLogging.class)
public class YtDynamicTableWriterPoolClientTest {
    private static final OffsetDateTime T_OFFSET_DTTM = OffsetDateTime.MIN;
    private RuntimeContext context;
    private RetryStrategy retryStrategy;
    private YtWriterOptions ytWriterOptions;
    private final Random random = new Random(42);

    @BeforeEach
    void setUp() {
        // N.B.: this can be replaced with real YT client (it'll require minor changes in current tests)
        // to provide integration tests
        context = Mockito.mock(RuntimeContext.class);
        Mockito.when(context.getMetricGroup()).thenReturn(UnregisteredMetricsGroup.createOperatorMetricGroup());
        retryStrategy = new ExponentialBackoffRetryStrategy(5, Duration.ZERO, Duration.ZERO);
        ytWriterOptions = YtWriterOptions.builder().build();
    }

    @SneakyThrows
    @ParameterizedTest
    @ValueSource(ints = {10, 100, 1_000_000})
    public void testWriteSingleThreadLight(int rowsCount) {
        testWriteSingleThread(singleTableData(rowsCount));
    }

    @SneakyThrows
    @ParameterizedTest
    @CsvSource({
            "1000000,   2",
            "1000000,   4",
    })
    public void testWriteMultiThreadEvenLight(int totalRows, int threadCount) {
        Assertions.assertEquals(0, totalRows % threadCount, "Incorrect test");
        testWriteMultiThreadBalanced(
                IntStream.range(0, threadCount)
                        .boxed()
                        .map(it -> singleTableData(totalRows / threadCount))
                        .collect(Collectors.toList()),
                threadCount);
    }

    @SneakyThrows
    @ParameterizedTest
    @CsvSource({
            "1000000,   2,      1",
            "1000000,   4,      10",
    })
    public void testWriteMultiThreadEvenPartitionLight(int totalRows, int threadCount, int partitionCount) {
        Assertions.assertEquals(0, totalRows % threadCount, "Incorrect test");
        testWriteMultiThreadBalanced(
                IntStream.range(0, threadCount)
                        .boxed()
                        .map(it -> partitionedData(totalRows / threadCount, partitionCount))
                        .collect(Collectors.toList()),
                threadCount
        );
    }

    @SneakyThrows
    @ParameterizedTest
    @CsvSource({
            "1000000,   2",
            "1000000,   50",
    })
    public void testWriteSingleThreadPartitionLight(int rowsCount, int partitionCount) {
        testWriteSingleThread(partitionedData(rowsCount, partitionCount));
    }

    /**
     * With no busy-veto, a writer that still holds buffered / uncommitted rows can be evicted once its
     * TTL elapses. Eviction must not drop that data: {@link YtDynamicTableWriter#close()} flushes the
     * buffer and commits the in-flight transaction before releasing resources.
     * <ol>
     * <li>Write a handful of rows so the writer is "busy" but nothing has been committed yet</li>
     * <li>Advance the ticker past the TTL and run cache maintenance</li>
     * <li>The writer is evicted, and all its rows end up committed</li>
     * </ol>
     */
    @SneakyThrows
    @Test
    void testEvictedWriterWaitsForActiveLeaseThenCommitsBufferedRows() {
        var ticker = new MutableTicker();
        Duration ttl = Duration.ofMillis(100);
        // Same-thread executor: eviction (and therefore close()) runs synchronously with cleanUp(),
        // so the committed-row count can be asserted deterministically.
        var cache = YtDynamicTableWriterPool.makeTestCache(ttl, ticker, Runnable::run);
        var client = new TestYtClient<>(
                new BasicEmulatingNodeComponent(),
                new StubFailingCountingTransactionComponent(
                        random,
                        // Always success
                        Iterators.cycle(true),
                        (transaction) -> {
                        }));

        WriterClassifier busy = WriterClassifier.plain("busy");
        int rowsToWrite = 5;
        try (var pool = makePool(TestPoolSettings.builder()
                .clientPool(CountingTestYtClientPool.ofSingle(client))
                .customCache(cache))) {
            try (var lease = pool.acquire(busy)) {
                for (int i = 0; i < rowsToWrite; i++) {
                    GenericRowData genericRowData = new GenericRowData(2);
                    genericRowData.setField(0, (long) i);
                    genericRowData.setField(1, TimestampData.fromInstant(OffsetDateTime.now().toInstant()));
                    lease.writer().write(genericRowData);
                }

                // Retire the cache entry while its writer is still leased by an operation.
                ticker.advance(ttl.multipliedBy(10));
                cache.cleanUp();

                Assertions.assertNull(cache.getIfPresent(busy.getTableName()),
                        "Writer past its TTL must be evicted from the cache");
                Assertions.assertEquals(0, client.transactions().getCommittedRows(),
                        "Eviction must not close a writer while an operation holds its lease");
            }

            Assertions.assertEquals(rowsToWrite, client.transactions().getCommittedRows(),
                    "Releasing the last lease of a retired writer must close it and commit its rows");
        }
    }

    /**
     * Complements {@link #testEvictedWriterWaitsForActiveLeaseThenCommitsBufferedRows()}: a writer that has already flushed all
     * its rows and whose TTL has elapsed must be evicted (and closed) by a cache maintenance pass.
     */
    @SneakyThrows
    @Test
    void testIdleWriterEviction() {
        var ticker = new MutableTicker();
        Duration ttl = Duration.ofMillis(100);
        var cache = YtDynamicTableWriterPool.makeTestCache(ttl, ticker, Runnable::run);
        var client = new TestYtClient<>(
                new BasicEmulatingNodeComponent(),
                new StubFailingCountingTransactionComponent(
                        random,
                        // Always success
                        Iterators.cycle(true),
                        (transaction) -> {
                        }));

        WriterClassifier idle = WriterClassifier.plain("idle");
        try (var pool = makePool(TestPoolSettings.builder()
                .clientPool(CountingTestYtClientPool.ofSingle(client))
                .customCache(cache))) {
            GenericRowData genericRowData = new GenericRowData(2);
            genericRowData.setField(0, 0L);
            genericRowData.setField(1, TimestampData.fromInstant(OffsetDateTime.now().toInstant()));
            pool.write(idle, genericRowData);

            // Flush everything, then let its TTL elapse.
            pool.finish();
            Assertions.assertNotNull(cache.getIfPresent(idle.getTableName()));

            ticker.advance(ttl.multipliedBy(2));
            cache.cleanUp();

            Assertions.assertNull(cache.getIfPresent(idle.getTableName()),
                    "Idle writer past its TTL must be evicted from the cache");
        }
    }

    @SneakyThrows
    @Test
    void testConcurrentAcquireCreatesSingleWriterGeneration() {
        var writer = Mockito.mock(YtDynamicTableWriter.class);
        var createdWriters = new AtomicInteger();
        var classifier = WriterClassifier.plain("shared");
        var executor = Executors.newFixedThreadPool(8);

        try (var pool = makePoolWithWriterFactory(null, ignored -> {
            createdWriters.incrementAndGet();
            return writer;
        })) {
            List<Future<?>> operations = IntStream.range(0, 32)
                    .mapToObj(ignored -> executor.submit(() -> pool.ensureWriter(classifier)))
                    .collect(Collectors.toList());

            for (Future<?> operation : operations) {
                operation.get(5, TimeUnit.SECONDS);
            }

            Assertions.assertEquals(1, createdWriters.get());
        } finally {
            executor.shutdownNow();
        }

        Mockito.verify(writer, Mockito.times(1)).close();
    }

    @SneakyThrows
    @Test
    void testAcquireAfterEvictionCreatesNewGenerationWithoutClosingLeasedWriter() {
        var ticker = new MutableTicker();
        Duration ttl = Duration.ofMillis(100);
        var cache = YtDynamicTableWriterPool.makeTestCache(ttl, ticker, Runnable::run);
        var oldWriter = Mockito.mock(YtDynamicTableWriter.class);
        var newWriter = Mockito.mock(YtDynamicTableWriter.class);
        var writers = List.of(oldWriter, newWriter);
        var generation = new AtomicInteger();
        var classifier = WriterClassifier.plain("recreated");

        try (var pool = makePoolWithWriterFactory(cache,
                ignored -> writers.get(generation.getAndIncrement()))) {
            try (var oldLease = pool.acquire(classifier)) {
                ticker.advance(ttl.multipliedBy(2));
                cache.cleanUp();

                Assertions.assertNull(cache.getIfPresent(classifier.getTableName()));
                Mockito.verify(oldWriter, Mockito.never()).close();

                pool.ensureWriter(classifier);

                Assertions.assertEquals(2, generation.get());
                Assertions.assertSame(newWriter, pool.getWriters().iterator().next());
                Mockito.verify(oldWriter, Mockito.never()).close();
                Mockito.verify(newWriter, Mockito.never()).close();
            }

            Mockito.verify(oldWriter, Mockito.times(1)).close();
            Mockito.verify(newWriter, Mockito.never()).close();
        }

        Mockito.verify(oldWriter, Mockito.times(1)).close();
        Mockito.verify(newWriter, Mockito.times(1)).close();
    }

    @SneakyThrows
    @Test
    void testPoolCloseWaitsForActiveLeaseAndRejectsNewOperations() {
        var writer = Mockito.mock(YtDynamicTableWriter.class);
        var classifier = WriterClassifier.plain("active");
        var executor = Executors.newSingleThreadExecutor();
        var closeStarted = new CountDownLatch(1);
        var pool = makePoolWithWriterFactory(null, ignored -> writer);
        var lease = pool.acquire(classifier);

        try {
            Future<?> closeFuture = executor.submit(() -> {
                closeStarted.countDown();
                pool.close();
            });
            Assertions.assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
            waitUntilPoolRejectsOperations(pool, classifier);

            Assertions.assertThrows(TimeoutException.class,
                    () -> closeFuture.get(100, TimeUnit.MILLISECONDS));
            Mockito.verify(writer, Mockito.never()).close();

            lease.close();
            closeFuture.get(5, TimeUnit.SECONDS);
            pool.close();

            Mockito.verify(writer, Mockito.times(1)).close();
        } finally {
            lease.close();
            pool.close();
            executor.shutdownNow();
        }
    }

    @SneakyThrows
    @Test
    void testFinishHoldsLeaseDuringEviction() {
        assertOperationHoldsLeaseDuringEviction(
                YtDynamicTableWriterPool::finish,
                YtDynamicTableWriter::finish);
    }

    @SneakyThrows
    @Test
    void testSnapshotStateHoldsLeaseDuringEviction() {
        long checkpointId = 42L;
        assertOperationHoldsLeaseDuringEviction(
                pool -> pool.snapshotState(checkpointId),
                writer -> writer.snapshotState(checkpointId));
    }

    @SneakyThrows
    @Test
    void testAsynchronousEvictionCloseFailureIsReportedToNextOperation() {
        var ticker = new MutableTicker();
        Duration ttl = Duration.ofMillis(100);
        var removalExecutor = new QueuedExecutor();
        var cache = YtDynamicTableWriterPool.makeTestCache(ttl, ticker, removalExecutor);
        var writer = Mockito.mock(YtDynamicTableWriter.class);
        Mockito.doThrow(new RuntimeException("close failed")).when(writer).close();

        try (var pool = makePoolWithWriterFactory(cache, ignored -> writer)) {
            pool.ensureWriter(WriterClassifier.plain("failing"));
            ticker.advance(ttl.multipliedBy(2));
            cache.cleanUp();
            Mockito.verify(writer, Mockito.never()).close();

            removalExecutor.runAll();
            Mockito.verify(writer, Mockito.times(1)).close();

            RuntimeException error = Assertions.assertThrows(RuntimeException.class, pool::finish);
            Assertions.assertEquals("close failed", error.getMessage());
        }
    }

    @Test
    void testPoolCloseReportsFailuresFromAllWriters() {
        var classifier1 = WriterClassifier.plain("table1");
        var classifier2 = WriterClassifier.plain("table2");
        var writer1 = Mockito.mock(YtDynamicTableWriter.class);
        var writer2 = Mockito.mock(YtDynamicTableWriter.class);
        Mockito.doThrow(new RuntimeException("close failed 1")).when(writer1).close();
        Mockito.doThrow(new RuntimeException("close failed 2")).when(writer2).close();
        Map<String, YtDynamicTableWriter> writers = Map.of(
                classifier1.getTableName(), writer1,
                classifier2.getTableName(), writer2);
        var pool = makePoolWithWriterFactory(null,
                classifier -> writers.get(classifier.getTableName()));

        pool.ensureWriter(classifier1);
        pool.ensureWriter(classifier2);

        RuntimeException error = Assertions.assertThrows(RuntimeException.class, pool::close);
        List<String> messages = Stream.concat(Stream.of(error), Stream.of(error.getSuppressed()))
                .map(Throwable::getMessage)
                .collect(Collectors.toList());

        Assertions.assertEquals(2, messages.size());
        Assertions.assertTrue(messages.contains("close failed 1"));
        Assertions.assertTrue(messages.contains("close failed 2"));
        Mockito.verify(writer1, Mockito.times(1)).close();
        Mockito.verify(writer2, Mockito.times(1)).close();

        pool.close();
        Mockito.verifyNoMoreInteractions(writer1, writer2);
    }

    @SneakyThrows
    @Disabled("slow, manual only")
    @Test
    public void testWriteSingleThreadPartitionHeavy() {
        testWriteSingleThread(partitionedData(100_000_000, 2));
    }

    @SneakyThrows
    @Disabled("slow, manual only")
    @Test
    public void testWriteSingleThreadHeavy() {
        testWriteSingleThread(singleTableData(1_000_000_000));
    }

    @SneakyThrows
    private long testWriteSingleThread(Stream<Pair<WriterClassifier, RowData>> data,
                                       CountingTestYtClientPool clientPool) {
        AtomicLong total = new AtomicLong(0);
        try (var pool = makePool(clientPool)) {
            data.forEach(pair -> {
                pool.write(pair.getKey(), pair.getValue());
                total.getAndIncrement();
            });
        }
        Assertions.assertEquals(total.get(), clientPool.getCommittedRows());
        return total.get();
    }

    private void testWriteSingleThread(Stream<Pair<WriterClassifier, RowData>> data) {
        testWriteSingleThread(data, new CountingTestYtClientPool(this::makeTestClient, 1));
    }

    @SneakyThrows
    private void testWriteMultiThreadBalanced(List<Stream<Pair<WriterClassifier, RowData>>> dataForThreads,
                                              int threadCount) {
        Preconditions.checkArgument(threadCount > 0);
        Preconditions.checkArgument(dataForThreads.size() == threadCount);
        ExecutorService service = Executors.newFixedThreadPool(threadCount);
        List<Future<Long>> tasks = new ArrayList<>();
        try (var totalClientPool = new CountingTestYtClientPool(this::makeTestClient, threadCount)) {
            for (int i = 0; i < threadCount; i++) {
                Stream<Pair<WriterClassifier, RowData>> stream = dataForThreads.get(i);
                var singleClientPool = CountingTestYtClientPool.ofSingle(totalClientPool.produce());
                tasks.add(service.submit(() -> testWriteSingleThread(stream, singleClientPool)));
            }
            long totalExpected = 0;
            for (Future<Long> task : tasks) {
                totalExpected += task.get(30, TimeUnit.SECONDS);
            }
            Assertions.assertEquals(totalExpected, totalClientPool.getCommittedRows());
        }
    }

    private Stream<Pair<WriterClassifier, RowData>> singleTableData(int totalRows) {
        WriterClassifier sample = WriterClassifier.plain("sample");
        return IntStream.range(0, totalRows)
                .mapToObj(i -> {
                    GenericRowData genericRowData = new GenericRowData(2);
                    genericRowData.setField(0, (long) i);
                    genericRowData.setField(1, TimestampData.fromInstant(T_OFFSET_DTTM.toInstant()));
                    return Pair.of(sample, genericRowData);
                });
    }

    private Stream<Pair<WriterClassifier, RowData>> partitionedData(int totalRows, int partitionCount) {
        OffsetDateTime startToday = OffsetDateTime.of(
                2024, 10, 10, 10, 10, 10, 10, ZoneOffset.UTC);
        PartitionConfig partitionConfig = new PartitionConfig(
                "date",
                PartitionScale.DAY,
                new YtPartitioningInstantRowDataConverter(new TimestampType()));
        return IntStream.range(0, totalRows)
                .mapToObj(i -> {
                    int partitionNum = random.nextInt(partitionCount);
                    OffsetDateTime partitionDttm = startToday.plus(partitionNum, ChronoUnit.DAYS);
                    WriterClassifier classifier = WriterClassifier.partition(partitionDttm.toInstant(),
                            partitionConfig);

                    GenericRowData genericRowData = new GenericRowData(2);
                    genericRowData.setField(0, (long) i);
                    genericRowData.setField(1, TimestampData.fromInstant(partitionDttm.toInstant()));

                    Assertions.assertEquals(
                            startToday.plus(partitionNum, ChronoUnit.DAYS).toLocalDate().toString(),
                            classifier.getTableName());

                    return Pair.of(classifier, genericRowData);
                });
    }

    private TestYtClient<BasicEmulatingNodeComponent, StubFailingCountingTransactionComponent> makeTestClient() {
        return new TestYtClient<>(
                new BasicEmulatingNodeComponent(),
                new StubFailingCountingTransactionComponent(
                        random,
                        new RandomWithFlipFalseBooleanIterator(retryStrategy.getNumRemainingRetries(), random),
                        (ignored) -> {
                        }));
    }

    private YtDynamicTableWriterPool makePool(YtClientPool<?> clientPool) {
        return makePool(TestPoolSettings.builder().clientPool(clientPool));
    }

    private YtDynamicTableWriterPool makePool(TestPoolSettings.TestPoolSettingsBuilder builder) {
        if (builder.schema == null || builder.logicalType == null) {
            builder.schema = "[{\"name\"=\"id\";\"type\"=\"int64\";};{\"name\"=\"date\";\"type\"=\"string\";}]";
            builder.logicalType = new RowType(
                    List.of(
                            new RowType.RowField("id", new BigIntType()),
                            new RowType.RowField("date", new TimestampType())
                    )
            );
        }
        TestPoolSettings settings = builder.build();
        RowDataToYtListConverters ytConverter = new RowDataToYtListConverters(TimestampFormat.ISO_8601);
        return new YtDynamicTableWriterPool(
                settings.getCustomCache(),
                settings.getClientPool()::produce,
                ytConverter.createConverter(settings.getLogicalType(),
                        YTreeTextSerializer.deserialize(settings.getSchema())),
                ComplexYtPath.builder().basePath("//home/ytsaurus/flink").tableName("tests").build(),
                settings.getSchema(),
                null,
                retryStrategy,
                context,
                YtTableAttributes.empty(),
                ReshardingConfig.builder()
                        .reshardStrategy(ReshardStrategy.NONE)
                        .build(),
                YtWriterOptions.builder().build(),
                new NoopLocksProvider(),
                null,
                null);
    }

    private YtDynamicTableWriterPool makePoolWithWriterFactory(
            Cache<String, YtDynamicTableWriterPool.WriterHandle> cache,
            Function<WriterClassifier, YtDynamicTableWriter> writerFactory) {
        var pool = Mockito.spy(makePool(TestPoolSettings.builder()
                .clientPool(CountingTestYtClientPool.ofSingle(makeTestClient()))
                .customCache(cache)));
        Mockito.doAnswer(invocation -> writerFactory.apply(invocation.getArgument(0)))
                .when(pool).prepareWriter(Mockito.any(WriterClassifier.class));
        return pool;
    }

    private void assertOperationHoldsLeaseDuringEviction(
            Consumer<YtDynamicTableWriterPool> operation,
            Consumer<YtDynamicTableWriter> writerOperation) throws Exception {
        var ticker = new MutableTicker();
        Duration ttl = Duration.ofMillis(100);
        var cache = YtDynamicTableWriterPool.makeTestCache(ttl, ticker, Runnable::run);
        var writer = Mockito.mock(YtDynamicTableWriter.class);
        var operationStarted = new CountDownLatch(1);
        var allowOperationToFinish = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();

        YtDynamicTableWriter blockingWriter = Mockito.doAnswer(invocation -> {
            operationStarted.countDown();
            Assertions.assertTrue(allowOperationToFinish.await(5, TimeUnit.SECONDS));
            return null;
        }).when(writer);
        writerOperation.accept(blockingWriter);

        try (var pool = makePoolWithWriterFactory(cache, ignored -> writer)) {
            pool.ensureWriter(WriterClassifier.plain("leased"));
            Future<?> operationFuture = executor.submit(() -> operation.accept(pool));
            Assertions.assertTrue(operationStarted.await(5, TimeUnit.SECONDS));

            ticker.advance(ttl.multipliedBy(2));
            cache.cleanUp();
            Mockito.verify(writer, Mockito.never()).close();

            allowOperationToFinish.countDown();
            operationFuture.get(5, TimeUnit.SECONDS);
            Mockito.verify(writer, Mockito.times(1)).close();
        } finally {
            allowOperationToFinish.countDown();
            executor.shutdownNow();
        }
    }

    private void waitUntilPoolRejectsOperations(YtDynamicTableWriterPool pool,
                                                WriterClassifier classifier) {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadlineNanos) {
            try {
                pool.ensureWriter(classifier);
            } catch (IllegalStateException e) {
                return;
            }
            Thread.yield();
        }
        Assertions.fail("Writer pool did not stop accepting operations");
    }

    @Test
    void testFinishReportsErrorsFromAllWriters() {
        WriterClassifier classifier1 = WriterClassifier.plain("table1");
        WriterClassifier classifier2 = WriterClassifier.plain("table2");
        var writer1 = Mockito.mock(YtDynamicTableWriter.class);
        var writer2 = Mockito.mock(YtDynamicTableWriter.class);
        Mockito.when(writer1.getPath()).thenReturn("//table1");
        Mockito.when(writer2.getPath()).thenReturn("//table2");
        Mockito.doThrow(new RuntimeException("Test error 1")).when(writer1).finish();
        Mockito.doThrow(new RuntimeException("Test error 2")).when(writer2).finish();
        Map<String, YtDynamicTableWriter> writers = new ConcurrentHashMap<>();
        writers.put(classifier1.getTableName(), writer1);
        writers.put(classifier2.getTableName(), writer2);

        try (var pool = makePoolWithWriterFactory(null,
                classifier -> writers.get(classifier.getTableName()))) {
            pool.ensureWriter(classifier1);
            pool.ensureWriter(classifier2);

            RuntimeException exception = Assertions.assertThrows(RuntimeException.class, pool::finish);

            Assertions.assertTrue(exception.getMessage().contains("Failure to finish 2 writer(-s)"));
            Assertions.assertTrue(exception.getMessage().contains("Writer at '"));
            Assertions.assertTrue(exception.getMessage().contains("Test error 1"));
            Assertions.assertTrue(exception.getMessage().contains("Test error 2"));
            Assertions.assertEquals(2, exception.getSuppressed().length);
        }
    }

    @Builder
    @Value
    private static class TestPoolSettings {
        String schema;
        LogicalType logicalType;
        YtClientPool<?> clientPool;
        Cache<String, YtDynamicTableWriterPool.WriterHandle> customCache;
    }

    /**
     * Manually advanced {@link Ticker} so cache expiry can be exercised deterministically, without
     * depending on wall-clock timing.
     */
    private static final class MutableTicker implements Ticker {
        private final AtomicLong nanos = new AtomicLong(0);

        @Override
        public long read() {
            return nanos.get();
        }

        void advance(Duration duration) {
            nanos.addAndGet(duration.toNanos());
        }
    }

    private static final class QueuedExecutor implements Executor {
        private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        void runAll() {
            Runnable task;
            while ((task = tasks.poll()) != null) {
                task.run();
            }
        }
    }
}
