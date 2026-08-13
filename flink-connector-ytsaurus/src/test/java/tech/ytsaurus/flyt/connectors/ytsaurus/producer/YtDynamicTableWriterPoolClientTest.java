package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.TemporalCache;

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

    @SneakyThrows
    @Test
    void testCacheCleanupWaitsForActiveWrite() {
        MutableTicker ticker = new MutableTicker();
        Duration ttl = Duration.ofMinutes(2);
        var cache = makeCache(ttl, ticker, ignored -> true);
        YtDynamicTableWriter writer = Mockito.mock(YtDynamicTableWriter.class);
        CountDownLatch writeStarted = new CountDownLatch(1);
        CountDownLatch allowWriteToFinish = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            writeStarted.countDown();
            Assertions.assertTrue(allowWriteToFinish.await(5, TimeUnit.SECONDS));
            return null;
        }).when(writer).write(Mockito.any());

        ExecutorService service = Executors.newFixedThreadPool(2);
        try (var pool = makePoolWithWriterFactory(
                cache,
                ignored -> writer)) {
            Assertions.assertSame(writer, pool.getOrAcquire(WriterClassifier.plain("active")));
            Future<?> write = service.submit(() -> pool.write(WriterClassifier.plain("active"), rowWithId(1)));
            Assertions.assertTrue(writeStarted.await(5, TimeUnit.SECONDS));

            ticker.advance(ttl.multipliedBy(2));
            Future<?> cleanup = service.submit(pool::cleanUpCache);

            Assertions.assertThrows(TimeoutException.class, () -> cleanup.get(100, TimeUnit.MILLISECONDS));
            Mockito.verify(writer, Mockito.never()).close();

            allowWriteToFinish.countDown();
            write.get(5, TimeUnit.SECONDS);
            cleanup.get(5, TimeUnit.SECONDS);

            Mockito.verify(writer).close();
            Assertions.assertEquals(0, pool.getCacheSize());
        } finally {
            allowWriteToFinish.countDown();
            service.shutdownNow();
        }
    }

    @Test
    void testEvictionCommitsBufferedRows() {
        MutableTicker ticker = new MutableTicker();
        Duration ttl = Duration.ofMinutes(2);
        CountingTestYtClientPool clients = new CountingTestYtClientPool(this::makeTestClient, 2);
        var cache = makeCache(ttl, ticker, ignored -> true);

        try (var pool = makePool(TestPoolSettings.builder()
                .clientPool(clients)
                .customCache(cache))) {
            pool.write(WriterClassifier.plain("evicted"), rowWithId(1));
            ticker.advance(ttl.multipliedBy(2));
            pool.cleanUpCache();

            Assertions.assertEquals(0, pool.getCacheSize());
            Assertions.assertEquals(1, clients.getCommittedRows());
        }
    }

    @Test
    void testAcquiredWriterRejectsWriteAfterEviction() {
        MutableTicker ticker = new MutableTicker();
        var cache = makeCache(Duration.ZERO, ticker, ignored -> true);
        CountingTestYtClientPool clients = new CountingTestYtClientPool(this::makeTestClient, 2);

        try (var pool = makePool(TestPoolSettings.builder()
                .clientPool(clients)
                .customCache(cache))) {
            YtDynamicTableWriter writer = pool.getOrAcquire(WriterClassifier.plain("evicted"));
            pool.cleanUpCache();

            IllegalStateException failure = Assertions.assertThrows(
                    IllegalStateException.class,
                    () -> writer.write(rowWithId(1)));
            Assertions.assertTrue(failure.getMessage().contains("closing or closed"));
        }
    }

    /**
     * A writer with an uncommitted transaction must remain in the cache even after its TTL.
     * Replacing it with another writer would lose data from the first transaction.
     */
    @SneakyThrows
    @Test
    void testLongCommitCacheEvict() {
        WriterClassifier longCommit = WriterClassifier.plain("longCommit");
        CountDownLatch transactionBegan = new CountDownLatch(1);
        CountDownLatch allowTransactionToFinish = new CountDownLatch(1);

        var defaultCache = YtDynamicTableWriterPool.makeDefaultCache();
        var cache = defaultCache.toBuilder()
                .ttl(Duration.ZERO, defaultCache.getExpirationCondition())
                .cleanupPeriod(Integer.MAX_VALUE, TimeUnit.DAYS)
                .build();
        var client = new TestYtClient<>(
                new BasicEmulatingNodeComponent(),
                new StubFailingCountingTransactionComponent(
                        random,
                        Iterators.cycle(true),
                        ignored -> {
                            try {
                                transactionBegan.countDown();
                                allowTransactionToFinish.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(e);
                            }
                        }));

        ExecutorService service = Executors.newSingleThreadExecutor();
        YtDynamicTableWriterPool pool = makePool(TestPoolSettings.builder()
                .clientPool(CountingTestYtClientPool.ofSingle(client))
                .customCache(cache));
        try {
            pool.write(longCommit, rowWithId(1));
            YtDynamicTableWriter writer = pool.getOrAcquire(longCommit);
            Future<?> snapshot = service.submit(() -> writer.snapshotState(1));
            Assertions.assertTrue(transactionBegan.await(5, TimeUnit.SECONDS));

            cache.cleanup();
            Assertions.assertEquals(1, cache.getSize());
            Assertions.assertSame(writer, pool.getOrAcquire(longCommit));

            allowTransactionToFinish.countDown();
            snapshot.get(5, TimeUnit.SECONDS);
            cache.cleanup();
            Assertions.assertEquals(0, cache.getSize());
        } finally {
            allowTransactionToFinish.countDown();
            pool.close();
            service.shutdownNow();
        }

        Assertions.assertEquals(1, client.transactions().getCommittedRows());
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

    private TemporalCache<String, YtDynamicTableWriter> makeCache(
            Duration ttl,
            MutableTicker ticker,
            Function<YtDynamicTableWriter, Boolean> expirationCondition) {
        return TemporalCache.<String, YtDynamicTableWriter>builder()
                .ttl(ttl, expirationCondition)
                .cleanupPeriod(Integer.MAX_VALUE, TimeUnit.DAYS)
                .removalListener(entry -> entry.getValue().close())
                .ticker(ticker)
                .build();
    }

    private YtDynamicTableWriterPool makePoolWithWriterFactory(
            TemporalCache<String, YtDynamicTableWriter> cache,
            Function<WriterClassifier, YtDynamicTableWriter> writerFactory) {
        YtDynamicTableWriterPool pool = Mockito.spy(makePool(TestPoolSettings.builder()
                .clientPool(CountingTestYtClientPool.ofSingle(makeTestClient()))
                .customCache(cache)));
        Mockito.doAnswer(invocation -> writerFactory.apply(invocation.getArgument(0)))
                .when(pool).prepareWriter(Mockito.any());
        return pool;
    }

    private RowData rowWithId(long id) {
        GenericRowData row = new GenericRowData(2);
        row.setField(0, id);
        row.setField(1, TimestampData.fromInstant(T_OFFSET_DTTM.toInstant()));
        return row;
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

    @Test
    void testMultipleOperationsErrorReporting() {
        var cache = TemporalCache.<String, YtDynamicTableWriter>builder()
                .ttl(1, TimeUnit.DAYS)
                .cleanupPeriod(Integer.MAX_VALUE, TimeUnit.DAYS)
                .build();
        var failingWriter1 = Mockito.mock(YtDynamicTableWriter.class);
        var failingWriter2 = Mockito.mock(YtDynamicTableWriter.class);
        Mockito.when(failingWriter1.getPath()).thenReturn("//table1");
        Mockito.when(failingWriter2.getPath()).thenReturn("//table2");
        Mockito.doThrow(new RuntimeException("Test error 1")).when(failingWriter1).close();
        Mockito.doThrow(new RuntimeException("Test error 2")).when(failingWriter2).close();
        cache.put("table1", failingWriter1);
        cache.put("table2", failingWriter2);

        var pool = makePool(TestPoolSettings.builder()
                .clientPool(CountingTestYtClientPool.ofSingle(makeTestClient()))
                .customCache(cache));
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class, pool::close);

        Assertions.assertTrue(exception.getMessage().contains("Failure to close 2 writer(-s)"));
        Assertions.assertTrue(exception.getMessage().contains("Writer at '"));
        Assertions.assertTrue(exception.getMessage().contains("Test error 1"));
        Assertions.assertTrue(exception.getMessage().contains("Test error 2"));
        Assertions.assertEquals(2, exception.getSuppressed().length);
        List<String> suppressedMessages = Stream.of(exception.getSuppressed())
                .map(Throwable::getMessage)
                .collect(Collectors.toList());
        Assertions.assertTrue(suppressedMessages.contains("Test error 1"));
        Assertions.assertTrue(suppressedMessages.contains("Test error 2"));
    }

    private static final class MutableTicker implements LongSupplier {
        private final AtomicLong nanos = new AtomicLong();

        @Override
        public long getAsLong() {
            return nanos.get();
        }

        private void advance(Duration duration) {
            nanos.addAndGet(duration.toNanos());
        }
    }

    @Builder
    @Value
    private static class TestPoolSettings {
        String schema;
        LogicalType logicalType;
        YtClientPool<?> clientPool;
        TemporalCache<String, YtDynamicTableWriter> customCache;
    }
}
