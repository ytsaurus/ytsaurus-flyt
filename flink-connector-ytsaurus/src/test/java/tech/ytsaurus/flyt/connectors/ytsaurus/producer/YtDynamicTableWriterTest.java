package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.concurrent.RetryStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.flyt.locks.noop.NoopLocksProvider;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtTableAttributes;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
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
                new NoopLocksProvider()
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

        doReturn(successCreateTable).when(ytDynamicTableWriter).tryCreateTable();
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

        doReturn(successCreateTable).when(ytDynamicTableWriter).tryCreateTable();
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

        verify(ytDynamicTableWriter, never()).tryCreateTable();
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
        verify(ytDynamicTableWriter, never()).tryCreateTable();

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
}
