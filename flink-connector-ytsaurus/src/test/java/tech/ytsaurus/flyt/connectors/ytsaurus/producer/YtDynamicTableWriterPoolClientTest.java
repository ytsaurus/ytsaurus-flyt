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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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

    /**
     * Checking behaviour in the following situation:
     * <ol>
     * <li>Have a writer in cache</li>
     * <li>Setup a long commit transaction</li>
     * <li>Simulate cache cleanup (writer must exist longer than cache TTL)</li>
     * <li>Check if the writer gets evicted</li>
     * </ol>
     * Expected result: Writer 'A' must not get evicted from cache until transaction commits.
     *                  This, in particular, implies that writer 'A' must not be replaced with any other writer.
     */
    @SneakyThrows
    @Test
    void testLongCommitCacheEvict() {
        ExecutorService service = Executors.newFixedThreadPool(1);
        WriterClassifier longCommit = WriterClassifier.plain("longCommit");
        AtomicLong transactionCount = new AtomicLong(0);
        CountDownLatch foreverTransactionBegan = new CountDownLatch(1);
        CountDownLatch cacheCleanupFinished = new CountDownLatch(1);
        AtomicReference<String> failMessage = new AtomicReference<>();

        var defaultCache = YtDynamicTableWriterPool.makeDefaultCache();
        var cache = Mockito.spy(defaultCache.toBuilder()
                .ttl(Duration.ZERO, defaultCache.getExpirationCondition())
                .cleanupPeriod(Integer.MAX_VALUE, TimeUnit.DAYS)
                .removalListener(entry -> failMessage.set(
                        entry.getKey() + " must not have been evicted from the cache! " +
                                "This is a data loss"))
                .build());
        var client = new TestYtClient<>(
                new BasicEmulatingNodeComponent(),
                new StubFailingCountingTransactionComponent(
                        random,
                        // Always success
                        Iterators.cycle(true),
                        (transaction) -> {
                            if (transactionCount.incrementAndGet() == 1) {
                                try {
                                    log.error("Blocking transaction began");
                                    foreverTransactionBegan.countDown();
                                    cacheCleanupFinished.await();
                                    log.debug("Unblock blocking transaction");
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }));


        try (var pool = makePool(TestPoolSettings.builder()
                .clientPool(CountingTestYtClientPool.ofSingle(client))
                .customCache(cache))) {
            service.submit(() -> {
                try {
                    foreverTransactionBegan.await();
                    log.debug("Cache cleanup began");
                    // this must not evict current writer (even though its expired)
                    // because it holds an uncommitted transaction
                    cache.cleanup();
                    log.debug("Cache cleanup finished");
                    cacheCleanupFinished.countDown();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            for (int i = 0; i < ytWriterOptions.getRowsInTransactionLimit(); i++) {
                GenericRowData genericRowData = new GenericRowData(2);
                genericRowData.setField(0, (long) i);
                genericRowData.setField(1, TimestampData.fromInstant(OffsetDateTime.now().toInstant()));
                pool.getOrAcquire(longCommit).write(genericRowData);
            }
        }

        Assertions.assertNull(failMessage.get());
        Mockito.verify(cache, Mockito.times(1)).cleanup();
        Assertions.assertEquals(
                ytWriterOptions.getRowsInTransactionLimit(),
                client.transactions().getCommittedRows());
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
                pool.getOrAcquire(pair.getKey()).write(pair.getValue());
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

    @Builder
    @Value
    private static class TestPoolSettings {
        String schema;
        LogicalType logicalType;
        YtClientPool<?> clientPool;
        TemporalCache<String, YtDynamicTableWriter> customCache;
    }
}
