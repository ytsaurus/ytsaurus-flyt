package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flyt.locks.noop.NoopLocksProvider;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardStrategy;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardingConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtTableAttributes;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionScale;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.YtPartitioningInstantRowDataConverter;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.PartitionScaleUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_ATTRIBUTE_SYMBOL;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_ENABLE_DYNAMIC_STORE_READ_ATTRIBUTE_NAME;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_EXPIRATION_TIME_ATTRIBUTE_NAME;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_PATH_SEP;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_TABLET_STATE_ATTRIBUTE_NAME;

public class YtDynamicTableWriterPoolTest {
    private static final String YT_UNMOUNTED_TABLET_STATE_VALUE = "unmounted";

    private YTsaurusClient mockedClient;

    private RuntimeContext context;

    private RetryStrategy retryStrategy;

    private final YtWriterOptions ytWriterOptions = YtWriterOptions.builder().build();

    @BeforeEach
    void setUp() {
        mockedClient = Mockito.mock(YTsaurusClient.class);
        context = Mockito.mock(RuntimeContext.class);
        Mockito.when(context.getMetricGroup())
                .thenReturn(UnregisteredMetricsGroup.createOperatorMetricGroup());
        retryStrategy = new ExponentialBackoffRetryStrategy(0, Duration.ZERO, Duration.ZERO);
    }

    private YtDynamicTableWriterPool makePool(ComplexYtPath path, String schema, LogicalType logicalType) {
        RowDataToYtListConverters ytConverter = new RowDataToYtListConverters(TimestampFormat.ISO_8601);
        return new YtDynamicTableWriterPool(
                () -> mockedClient,
                ytConverter.createConverter(logicalType, YTreeTextSerializer.deserialize(schema)),
                path,
                schema,
                null,
                retryStrategy,
                context,
                YtTableAttributes.empty(),
                ReshardingConfig.builder()
                        .reshardStrategy(ReshardStrategy.NONE)
                        .build(),
                ytWriterOptions,
                new NoopLocksProvider()
        );
    }

    private YtDynamicTableWriterPool makePool(String basePath, String schema, LogicalType logicalType) {
        return makePool(ComplexYtPath.builder().basePath(basePath).build(), schema, logicalType);
    }

    @Test
    void poolAcquireConnectionThenCheckPathSuccess() {
        setYtPathAvailable("home", "smth", "__tests__", "tests", "sample");
        mockMountAny();
        try (var pool = makePool("//home/smth/__tests__/tests", "[]", new IntType())) {
            var writer = pool.getOrAcquire(WriterClassifier.plain("sample"));
            assertEquals("//home/smth/__tests__/tests/sample", writer.toString().split(" ")[3]);
        }
    }

    @Test
    void poolAcquireConnectionForbiddenPathSuccess() {
        setYtPathAvailable("__non_existent__");
        try (var pool = makePool("//home/smth/__tests__/tests", "[]", new IntType())) {
            pool.getOrAcquire(WriterClassifier.plain("sample"));
            fail("No exception was thrown acquiring writer for a forbidden path");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Insufficient permissions"));
        }
    }

    @SneakyThrows
    @Test
    void poolAcquireConnectionWrite() {
        String schema = "[{\"name\"=\"id\";\"type\"=\"int64\";};{\"name\"=\"date\";\"type\"=\"string\";}]";
        TableSchema tableSchema = TableSchema.fromYTree(YTreeTextSerializer.deserialize(schema));
        LogicalType logicalType = new RowType(
                List.of(
                        new RowType.RowField("id", new BigIntType()),
                        new RowType.RowField("date", new TimestampType())
                )
        );

        setYtPathAvailable("values");
        mockMountAny();
        ApiServiceTransaction transaction = Mockito.mock(ApiServiceTransaction.class);
        Mockito.when(mockedClient.startTransaction(any(StartTransaction.class)))
                .thenReturn(CompletableFuture.completedFuture(transaction));

        AtomicReference<List<ModifyRowsRequest>> rows = new AtomicReference<>(new ArrayList<>());
        Mockito.when(transaction.modifyRows(any(ModifyRowsRequest.Builder.class)))
                .thenAnswer((Answer<CompletableFuture<Void>>) invocation -> {
                    ModifyRowsRequest.Builder requestBuilder = invocation.getArgument(0);
                    ModifyRowsRequest request = requestBuilder.build();
                    rows.get().add(request);

                    request.getRows().forEach(row ->
                            assertEquals("2022-10-30T10:10:10",
                                    row.toYTreeMap(tableSchema).getString("date")));

                    return CompletableFuture.completedFuture(null);
                });
        Mockito.when(transaction.commit()).thenReturn(CompletableFuture.completedFuture(null));

        try (var pool = makePool("/", schema, logicalType)) {
            var writer = pool.getOrAcquire(WriterClassifier.plain("values"));
            for (int i = 0; i < ytWriterOptions.getRowsInModificationLimit() + 1; i++) {
                GenericRowData genericRowData = new GenericRowData(RowKind.INSERT, 2);
                // id
                genericRowData.setField(0, (long) i);
                // date
                genericRowData.setField(1, TimestampData.fromInstant(
                        LocalDateTime
                                .of(2022, 10, 30, 10, 10, 10)
                                .toInstant(ZoneOffset.UTC)
                ));
                writer.write(genericRowData);
            }
            assertEquals(1, rows.get().size());
        }
    }

    @Test
    void testFullTablePathCreated() {
        String schema = "[{\"name\"=\"id\";\"type\"=\"int64\";};{\"name\"=\"date\";\"type\"=\"string\";}]";
        LogicalType logicalType = new RowType(
                List.of(
                        new RowType.RowField("id", new BigIntType()),
                        new RowType.RowField("date", new TimestampType())
                )
        );

        var path = ComplexYtPath.builder()
                .basePath("//home/my_table")
                .tableName("my_table")
                .build();

        setYtPathAvailable(false, path);
        mockMountSpecific(path.getFullPath());

        Mockito.when(mockedClient.createNode(Mockito.<CreateNode>any()))
                .thenAnswer((Answer<CompletableFuture<GUID>>) invocation -> {
                    CreateNode request = invocation.getArgument(0);
                    assertEquals(path.getFullPath(), request.toBuilder().getPath().toStableString());
                    assertEquals(CypressNodeType.TABLE, request.getType());
                    assertTrue(request.isRecursive());
                    return CompletableFuture.completedFuture(GUID.create());
                });
        Mockito.when(mockedClient.setNode(
                        eq(path.getFullPathWithAttribute(YT_ENABLE_DYNAMIC_STORE_READ_ATTRIBUTE_NAME)),
                        Mockito.<YTreeNode>any()))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(null));
        Mockito.when(mockedClient.mountTableAndWaitTablets(any(MountTable.class)))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(null));

        try (var pool = makePool(path, schema, logicalType)) {
            pool.createFullPathTable();
        }

        Mockito.verify(mockedClient, Mockito.times(1))
                .mountTableAndWaitTablets(any(MountTable.class));
    }

    @Test
    void testPartitionTtlDayCntMinBound() {
        OffsetDateTime current = OffsetDateTime.of(
                2024, 10, 10, 10, 10, 10, 10, ZoneOffset.UTC);
        testPartition(
                current,
                current.minus(10, ChronoUnit.YEARS).toInstant(),
                current.plus(7, ChronoUnit.DAYS),
                PartitionConfig.builder()
                        .partitionMinTtl(7)
                        .partitionTtlDayCnt(1)
                        .partitionScale(PartitionScale.DAY));
    }

    @Test
    void testPartitionTtlInDaysFromCreationMinBound() {
        OffsetDateTime current = OffsetDateTime.of(
                2024, 10, 10, 10, 10, 10, 10, ZoneOffset.UTC);
        testPartition(
                current,
                current.toInstant(),
                current.plus(7, ChronoUnit.DAYS),
                PartitionConfig.builder()
                        .partitionMinTtl(7)
                        .partitionTtlInDaysFromCreation(3)
                        .partitionScale(PartitionScale.DAY));
    }

    @Test
    void testPartitionTtlFromDays() {
        OffsetDateTime current = OffsetDateTime.of(
                2024, 10, 10, 10, 10, 10, 10, ZoneOffset.UTC);
        testPartition(
                current,
                current.toInstant(),
                PartitionScaleUtils.getEnd(current.toInstant(), PartitionScale.DAY)
                        .plus(100, ChronoUnit.DAYS),
                PartitionConfig.builder()
                        .partitionTtlDayCnt(100)
                        .partitionScale(PartitionScale.DAY));
    }

    @Test
    void testPartitionTtlInDaysFromCreation() {
        OffsetDateTime current = OffsetDateTime.of(
                2024, 10, 10, 10, 10, 10, 10, ZoneOffset.UTC);
        testPartition(
                current,
                current.toInstant(),
                current.plus(100, ChronoUnit.DAYS),
                PartitionConfig.builder()
                        .partitionTtlInDaysFromCreation(100)
                        .partitionScale(PartitionScale.DAY));
    }

    @Test
    void testBasePathCreated() {
        String schema = "[{\"name\"=\"id\";\"type\"=\"int64\";};{\"name\"=\"date\";\"type\"=\"string\";}]";
        LogicalType logicalType = new RowType(
                List.of(
                        new RowType.RowField("id", new BigIntType()),
                        new RowType.RowField("date", new TimestampType())
                )
        );

        var path = ComplexYtPath.builder()
                .basePath("//home/something")
                .build();

        setYtPathAvailable(path.getBasePath(), false);
        Mockito.when(mockedClient.createNode(Mockito.<CreateNode>any()))
                .thenAnswer((Answer<CompletableFuture<GUID>>) invocation -> {
                    CreateNode request = invocation.getArgument(0);
                    assertEquals(CypressNodeType.MAP, request.getType());
                    assertTrue(request.isRecursive());
                    return CompletableFuture.completedFuture(GUID.create());
                });

        try (var pool = makePool(path, schema, logicalType)) {
            pool.createBasePathMapNode();
        }

        Mockito.verify(mockedClient, Mockito.times(1))
                .existsNode(path.getBasePath());
        Mockito.verify(mockedClient, Mockito.times(1))
                .createNode(Mockito.<CreateNode>any());
    }

    private void testPartition(
            OffsetDateTime current,
            Instant rowInstant,
            OffsetDateTime expected,
            PartitionConfig.PartitionConfigBuilder partitionConfigBuilder) {
        try (MockedStatic<OffsetDateTime> odt = mockStatic(OffsetDateTime.class, Mockito.CALLS_REAL_METHODS)) {
            odt.when(() -> OffsetDateTime.now(Mockito.<ZoneOffset>any())).thenReturn(current);
            String schema = "[{\"name\"=\"id\";\"type\"=\"int64\";};{\"name\"=\"date\";\"type\"=\"string\";}]";
            LogicalType logicalType = new RowType(
                    List.of(
                            new RowType.RowField("id", new BigIntType()),
                            new RowType.RowField("date", new TimestampType())
                    )
            );

            var path = ComplexYtPath.builder()
                    .basePath("//home/my_table")
                    .tableName("my_table")
                    .build();

            mockMountAny();

            Mockito.when(mockedClient.existsNode(Mockito.<String>any()))
                    .thenAnswer(invocation -> CompletableFuture.completedFuture(false));
            Mockito.when(mockedClient.createNode(Mockito.<CreateNode>any()))
                    .thenAnswer(invocation -> CompletableFuture.completedFuture(null));
            Mockito.when(mockedClient.setNode(any(), Mockito.<YTreeNode>any()))
                    .thenAnswer(invocation -> {
                        if (invocation.<String>getArgument(0).endsWith(YT_EXPIRATION_TIME_ATTRIBUTE_NAME)) {
                            Assertions.assertEquals(
                                    invocation.<YTreeNode>getArgument(1),
                                    YTree.stringNode(PartitionScaleUtils.formatYt(expected)));
                        }
                        return CompletableFuture.completedFuture(null);
                    });
            Mockito.when(mockedClient.mountTableAndWaitTablets(any(MountTable.class)))
                    .thenAnswer(invocation -> CompletableFuture.completedFuture(null));

            try (var pool = makePool(path, schema, logicalType)) {
                pool.getOrAcquire(WriterClassifier.partition(rowInstant, partitionConfigBuilder
                        .converter(new YtPartitioningInstantRowDataConverter(new TimestampType(3)))
                        .build()));
            }

            Mockito.verify(mockedClient, Mockito.times(1))
                    .setNode(endsWith(YT_EXPIRATION_TIME_ATTRIBUTE_NAME), Mockito.<YTreeNode>any());
        }
    }

    private void mockMountSpecific(String path) {
        Mockito.when(mockedClient
                        .getNode(endsWith(YT_PATH_SEP + YT_ATTRIBUTE_SYMBOL + YT_TABLET_STATE_ATTRIBUTE_NAME)))
                .thenReturn(
                        CompletableFuture.completedFuture(YTree.stringNode(YT_UNMOUNTED_TABLET_STATE_VALUE))
                );
        Mockito.when(mockedClient.mountTable(eq(path), any(), anyBoolean(), anyBoolean(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private void mockMountAny() {
        Mockito.when(mockedClient
                        .getNode(endsWith(YT_PATH_SEP + YT_ATTRIBUTE_SYMBOL + YT_TABLET_STATE_ATTRIBUTE_NAME)))
                .thenReturn(
                        CompletableFuture.completedFuture(YTree.stringNode(YT_UNMOUNTED_TABLET_STATE_VALUE))
                );
        Mockito.when(mockedClient.mountTable(anyString(), any(), anyBoolean(), anyBoolean(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        Mockito.when(mockedClient.mountTableAndWaitTablets(any(MountTable.class)))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(null));
    }

    private void setYtPathAvailable(boolean exists, String... parts) {
        StringBuilder builder = new StringBuilder(YT_PATH_SEP);
        Set<String> allowedPaths = new HashSet<>();

        for (String part : parts) {
            builder.append(YT_PATH_SEP).append(part);
            allowedPaths.add(builder.toString());
        }

        Mockito.when(mockedClient.existsNode(anyString()))
                .thenAnswer((Answer<CompletableFuture<Boolean>>) invocation -> {
                    if (allowedPaths.contains(invocation.getArgument(0).toString())) {
                        return CompletableFuture.completedFuture(exists);
                    }
                    return CompletableFuture.failedFuture(new IllegalAccessError(String.format(
                            "Insufficient permissions for %s (allowed=%s)",
                            invocation.getArgument(0).toString(),
                            String.join(", ", allowedPaths))));
                });
    }

    private void setYtPathAvailable(boolean exists, ComplexYtPath path) {
        setYtPathAvailable(path.getFullPath(), exists);
    }

    private void setYtPathAvailable(String... parts) {
        setYtPathAvailable(true, parts);
    }

    private void setYtPathAvailable(String fullPath) {
        setYtPathAvailable(fullPath.replace("//", "").split("/"));
    }

    private void setYtPathAvailable(String fullPath, boolean exists) {
        setYtPathAvailable(exists, fullPath.replace("//", "").split("/"));
    }
}
