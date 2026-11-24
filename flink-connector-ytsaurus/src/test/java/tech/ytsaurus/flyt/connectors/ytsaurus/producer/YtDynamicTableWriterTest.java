package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.flyt.connectors.datametrics.DataMetricsWriterDelegate;
import tech.ytsaurus.flyt.connectors.datametrics.NoopDataMetricsWriterDelegate;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.TrackableField;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtTableAttributes;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.TrackableFieldDataConverter;
import tech.ytsaurus.flyt.locks.noop.NoopLocksProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_MOUNTED_TABLET_STATE_VALUE;

class YtDynamicTableWriterTest {

    @Mock
    private YTsaurusClient ytClient;

    @Mock
    private YtWriterOptions ytWriterOptions;

    @Mock
    private ComplexYtPath path;

    @Mock
    private RowDataToYtListConverters.RowDataToYtMapConverter ytConverter;

    // Other necessary mocks
    @Mock
    private WriterClassifier writerClassifier;
    @Mock
    private RetryStrategy retryStrategy;
    @Mock
    private RetryStrategy locksRetryStrategy;
    @Mock
    private RuntimeContext runtimeContext;
    @Mock
    private MetricsSupplier metricsSupplier;
    @Mock
    private YtTableAttributes tableAttributes;

    private YtDynamicTableWriter ytDynamicTableWriter;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        // Create WriterYtInfo
        WriterYtInfo writerYtInfo = new WriterYtInfo(path, ytClient, "schema");

        // Initialize the tested class manually
        ytDynamicTableWriter = new YtDynamicTableWriter(
                ytConverter,
                writerYtInfo,
                null, // TrackableField
                writerClassifier,
                retryStrategy,
                locksRetryStrategy,
                runtimeContext,
                metricsSupplier,
                tableAttributes,
                null, // ReshardTable
                ytWriterOptions,
                new NoopLocksProvider(),
                NoopDataMetricsWriterDelegate.INSTANCE,
                null
        );
        ytDynamicTableWriter = Mockito.spy(ytDynamicTableWriter);
        doReturn(false).when(ytDynamicTableWriter).isTableMounted();
    }


    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void createAndMountTableIfNeeded_whenCreateNeededAndMountModeAlways(boolean successCreateTable) {
        when(path.getFullPath()).thenReturn("path");
        when(ytClient.existsNode(anyString())).thenReturn(CompletableFuture.completedFuture(false));
        when(ytWriterOptions.getMountMode()).thenReturn(MountMode.ALWAYS);

        doReturn(successCreateTable).when(ytDynamicTableWriter).tryCreateAndConfigureTheTable();
        doNothing().when(ytDynamicTableWriter).mountIfUnmounted();

        ytDynamicTableWriter.createAndMountTableIfNeeded();

        verify(ytClient, times(2)).existsNode("path");
        verify(ytDynamicTableWriter, times(1)).mountIfUnmounted();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void createAndMountTableIfNeeded_whenCreateNeededAndMountModeOnCreate(boolean successCreateTable) {
        when(path.getFullPath()).thenReturn("path");
        when(ytClient.existsNode(anyString())).thenReturn(CompletableFuture.completedFuture(false));
        when(ytWriterOptions.getMountMode()).thenReturn(MountMode.ON_CREATE);

        doReturn(successCreateTable).when(ytDynamicTableWriter).tryCreateAndConfigureTheTable();
        if (!successCreateTable) {
            doNothing().when(ytDynamicTableWriter).waitUntilMounted(anyLong());
        }
        doNothing().when(ytDynamicTableWriter).mountIfUnmounted();

        ytDynamicTableWriter.createAndMountTableIfNeeded();

        verify(ytClient, times(2)).existsNode("path");
        if (successCreateTable) {
            verify(ytDynamicTableWriter).mountIfUnmounted();
        } else {
            verify(ytDynamicTableWriter, never()).mountIfUnmounted();
        }
    }

    @Test
    void createAndMountTableIfNeeded_whenCreateNotNeededAndMountModeAlways() {
        when(path.getFullPath()).thenReturn("path");
        when(ytClient.existsNode(anyString())).thenReturn(CompletableFuture.completedFuture(true));
        when(ytWriterOptions.getMountMode()).thenReturn(MountMode.ALWAYS);

        verify(ytDynamicTableWriter, never()).tryCreateAndConfigureTheTable();
        doNothing().when(ytDynamicTableWriter).mountIfUnmounted();

        ytDynamicTableWriter.createAndMountTableIfNeeded();

        verify(ytClient).existsNode("path");
        verify(ytDynamicTableWriter).mountIfUnmounted();
    }

    @Test
    void createAndMountTableIfNeeded_whenCreateNotNeededAndMountModeOnCreate() {
        when(path.getFullPath()).thenReturn("path");
        when(ytClient.existsNode(anyString())).thenReturn(CompletableFuture.completedFuture(true));
        when(ytWriterOptions.getMountMode()).thenReturn(MountMode.ON_CREATE);

        doNothing().when(ytDynamicTableWriter).waitUntilMounted(YtDynamicTableWriter.WAIT_MOUNTING_TIMEOUT_MS);
        verify(ytDynamicTableWriter, never()).tryCreateAndConfigureTheTable();

        ytDynamicTableWriter.createAndMountTableIfNeeded();

        verify(ytClient).existsNode("path");
        verify(ytDynamicTableWriter, never()).mountIfUnmounted();
    }

    @Test
    void waitUntilMounted_shouldReturnImmediately_whenTableAlreadyMounted() {
        // Arrange
        doReturn(YT_MOUNTED_TABLET_STATE_VALUE).when(ytDynamicTableWriter).getTableState();

        // Act
        ytDynamicTableWriter.waitUntilMounted(4100);
        // Assert
        verify(ytDynamicTableWriter, times(1)).getTableState();
    }

    @Test
    void waitUntilMounted_shouldWaitUntilMounted() {
        // Arrange
        doReturn("unmounted")
                .doReturn("unmounted")
                .doReturn(YT_MOUNTED_TABLET_STATE_VALUE)
                .when(ytDynamicTableWriter).getTableState();


        // Act
        ytDynamicTableWriter.waitUntilMounted(4100);
        // Assert
        verify(ytDynamicTableWriter, times(3)).getTableState();
    }

    @Test
    void waitUntilMounted_shouldThrowTimeoutException_whenTimeoutReached() {
        // Arrange
        doReturn("unmounted").when(ytDynamicTableWriter).getTableState();
        // Act
        ytDynamicTableWriter.waitUntilMounted(4100);
        // Assert
        verify(ytDynamicTableWriter, atLeast(4)).getTableState();
    }

    @Test
    void delegatesBuffersAndPreservesCheckpointFinishFlushAndCommitMetrics() {
        when(path.getFullPath()).thenReturn("//tmp/table");
        when(path.getClusterName()).thenReturn("test");
        when(runtimeContext.getMetricGroup()).thenReturn(UnregisteredMetricsGroup.createOperatorMetricGroup());
        ApiServiceTransaction transaction = mock(ApiServiceTransaction.class);
        when(ytClient.startTransaction(any(StartTransaction.class)))
                .thenReturn(CompletableFuture.completedFuture(transaction));
        when(transaction.modifyRows(any(ModifyRowsRequest.Builder.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(transaction.commit()).thenReturn(CompletableFuture.completedFuture(null));
        DataMetricsWriterDelegate dataMetrics = mock(DataMetricsWriterDelegate.class);
        MetricsSupplier recordedMetrics = new MetricsSupplier("test");
        YtWriterOptions options = YtWriterOptions.builder()
                .rowsInModificationLimit(2)
                .rowsInTransactionLimit(5)
                .commitTransactionPeriod(Duration.ofHours(1))
                .flushModificationPeriod(Duration.ofHours(1))
                .transactionTimeout(Duration.ofSeconds(3))
                .build();
        YtDynamicTableWriter writer = Mockito.spy(new YtDynamicTableWriter(
                (reuse, value) -> Map.of("id", ((RowData) value).getLong(0)),
                new WriterYtInfo(path, ytClient, "[{name=id;type=int64;sort_order=ascending;}]"),
                null,
                WriterClassifier.plain("table"),
                new FixedRetryStrategy(0, Duration.ZERO),
                new FixedRetryStrategy(0, Duration.ZERO),
                runtimeContext,
                recordedMetrics,
                YtTableAttributes.empty(),
                null,
                options,
                new NoopLocksProvider(),
                dataMetrics,
                null));
        doNothing().when(writer).createAndMountTableIfNeeded();
        writer.open();
        try {
            writer.write(GenericRowData.of(1L));
            writer.write(GenericRowData.of(2L));
            verify(transaction, never()).modifyRows(any(ModifyRowsRequest.Builder.class));
            verify(transaction, never()).commit();
            assertThat(writer.isBusy()).isTrue();

            writer.snapshotState(1);
            verify(dataMetrics).onCommit();
            assertThat(recordedMetrics.getMetric("sumCommittedRows").get()).isEqualTo(2);
            assertThat(writer.isBusy()).isFalse();
            writer.write(GenericRowData.of(3L));
            // close() releases only; finish() is what drains the last row, as Flink calls it before close()
            writer.finish();
        } finally {
            writer.close();
        }

        verify(dataMetrics, times(3)).onRecord(any(RowData.class));
        verify(dataMetrics, times(2)).onCommit();
        verify(transaction, times(2)).commit();
        verify(ytClient).close();
        assertThat(recordedMetrics.getMetric("sumCommittedRows").get()).isEqualTo(3);
        assertThat(recordedMetrics.getMetric("sumFailedRows").get()).isZero();
        assertThat(recordedMetrics.getMetric("lastCommitTimestamp").get()).isEqualTo(-1);
    }

    @Test
    void lastTrackedFieldChangesOnlyAfterCommitAndClosesWithMetrics() {
        when(path.getFullPath()).thenReturn("//tmp/table");
        when(path.getClusterName()).thenReturn("test");
        when(runtimeContext.getMetricGroup()).thenReturn(UnregisteredMetricsGroup.createOperatorMetricGroup());
        ApiServiceTransaction transaction = mock(ApiServiceTransaction.class);
        when(ytClient.startTransaction(any(StartTransaction.class)))
                .thenReturn(CompletableFuture.completedFuture(transaction));
        when(transaction.modifyRows(any(ModifyRowsRequest.Builder.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(transaction.commit()).thenReturn(CompletableFuture.completedFuture(null));
        MetricsSupplier recordedMetrics = new MetricsSupplier("test");
        TimestampType timestampType = new TimestampType(3);
        TrackableField trackableField = new TrackableField("event_time", 1, timestampType.getTypeRoot(),
                new TrackableFieldDataConverter(timestampType));
        YtWriterOptions options = YtWriterOptions.builder()
                .rowsInModificationLimit(100)
                .rowsInTransactionLimit(100)
                .commitTransactionPeriod(Duration.ofHours(1))
                .flushModificationPeriod(Duration.ofHours(1))
                .transactionTimeout(Duration.ofSeconds(3))
                .build();
        YtDynamicTableWriter writer = Mockito.spy(new YtDynamicTableWriter(
                (reuse, value) -> Map.of("id", ((RowData) value).getLong(0),
                        "event_time", ((RowData) value).getTimestamp(1, 3).getMillisecond()),
                new WriterYtInfo(path, ytClient,
                        "[{name=id;type=int64;sort_order=ascending;};{name=event_time;type=int64;}]"),
                trackableField,
                WriterClassifier.plain("table"),
                new FixedRetryStrategy(0, Duration.ZERO),
                new FixedRetryStrategy(0, Duration.ZERO),
                runtimeContext,
                recordedMetrics,
                YtTableAttributes.empty(),
                null,
                options,
                new NoopLocksProvider(),
                NoopDataMetricsWriterDelegate.INSTANCE,
                null));
        doNothing().when(writer).createAndMountTableIfNeeded();
        writer.open();
        try {
            writer.write(GenericRowData.of(1L, TimestampData.fromEpochMillis(100L)));
            writer.snapshotState(1);
            assertThat(recordedMetrics.getMetric("lastTrackedField").get()).isEqualTo(100L);

            writer.write(GenericRowData.of(2L, TimestampData.fromEpochMillis(200L)));
            verify(transaction).commit();
            assertThat(recordedMetrics.getMetric("lastTrackedField").get()).isEqualTo(100L);
            assertThat(recordedMetrics.getMetric("trackedField").get()).isEqualTo(100L);

            writer.snapshotState(2);
            assertThat(recordedMetrics.getMetric("lastTrackedField").get()).isEqualTo(200L);
        } finally {
            writer.close();
        }

        assertThat(recordedMetrics.getMetric("lastTrackedField").get()).isEqualTo(-1L);
        assertThat(recordedMetrics.getMetric("trackedField").get()).isEqualTo(-1L);
        assertThat(recordedMetrics.getMetric("lastCommitTimestamp").get()).isEqualTo(-1L);
    }
}
