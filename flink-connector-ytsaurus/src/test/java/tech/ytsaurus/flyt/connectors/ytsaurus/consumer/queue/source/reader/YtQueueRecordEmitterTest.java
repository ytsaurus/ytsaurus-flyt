package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueueRawRecord;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplitState;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.table.YtQueueColumnValueDeserializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.YtQueueTestFixtures.split;

class YtQueueRecordEmitterTest {
    @Test
    void advancesOffsetOnlyAfterSuccessfulCollect() throws Exception {
        UnversionedRow row = mock(UnversionedRow.class);
        TableSchema schema = mock(TableSchema.class);
        YtQueueSplitState state = new YtQueueSplitState(split("queue-id", 2, 7));
        SourceOutput<String> output = output();
        YtQueueRecordEmitter<String> emitter = new YtQueueRecordEmitter<>((ignoredRow, ignoredSchema) -> "value");

        emitter.emitRecord(new YtQueueRawRecord(2, 7, row, schema), output, state);

        verify(output).collect("value");
        assertThat(state.getNextOffset()).isEqualTo(8);
    }

    @Test
    void partitionMismatchFailsWithoutCollectingOrAdvancingOffset() {
        UnversionedRow row = mock(UnversionedRow.class);
        TableSchema schema = mock(TableSchema.class);
        YtQueueSplitState state = new YtQueueSplitState(split("queue-id", 2, 7));
        SourceOutput<String> output = output();
        YtQueueRecordEmitter<String> emitter = new YtQueueRecordEmitter<>((ignoredRow, ignoredSchema) -> "value");

        assertThatThrownBy(() -> emitter.emitRecord(
                new YtQueueRawRecord(3, 7, row, schema),
                output,
                state
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Record partition does not match split state");

        verify(output, never()).collect(org.mockito.ArgumentMatchers.any());
        assertThat(state.getNextOffset()).isEqualTo(7);
    }

    @Test
    void leavesOffsetUnchangedWhenCollectFails() {
        UnversionedRow row = mock(UnversionedRow.class);
        TableSchema schema = mock(TableSchema.class);
        YtQueueSplitState state = new YtQueueSplitState(split("queue-id", 0, 11));
        SourceOutput<String> output = output();
        RuntimeException failure = new RuntimeException("output failed");
        YtQueueRecordEmitter<String> emitter = new YtQueueRecordEmitter<>((ignoredRow, ignoredSchema) -> "value");
        org.mockito.Mockito.doThrow(failure).when(output).collect("value");

        assertThatThrownBy(() -> emitter.emitRecord(
                new YtQueueRawRecord(0, 11, row, schema),
                output,
                state
        )).isSameAs(failure);

        assertThat(state.getNextOffset()).isEqualTo(11);
    }

    @Test
    void advancesOffsetWhenDeserializerSkipsRecord() throws Exception {
        UnversionedRow row = mock(UnversionedRow.class);
        TableSchema schema = mock(TableSchema.class);
        YtQueueSplitState state = new YtQueueSplitState(split("queue-id", 0, 3));
        SourceOutput<String> output = output();
        YtQueueRecordEmitter<String> emitter = new YtQueueRecordEmitter<>((ignoredRow, ignoredSchema) -> null);

        emitter.emitRecord(new YtQueueRawRecord(0, 3, row, schema), output, state);

        verify(output, never()).collect(org.mockito.ArgumentMatchers.any());
        assertThat(state.getNextOffset()).isEqualTo(4);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void advancesPastDecompressionErrorsOnlyWhenIgnoringIsEnabled(
            boolean ignoreDecompressionErrors) throws Exception {
        TableSchema schema = TableSchema.builder()
                .addValue("value", ColumnValueType.STRING)
                .addValue("codec", ColumnValueType.STRING)
                .build();
        UnversionedRow corruptedRow = new UnversionedRow(List.of(
                new UnversionedValue(0, ColumnValueType.STRING, false, new byte[Long.BYTES + 1]),
                new UnversionedValue(1, ColumnValueType.STRING, false,
                        "zstd_6".getBytes(StandardCharsets.UTF_8))));
        YtQueueColumnValueDeserializer<String> deserializer = new YtQueueColumnValueDeserializer<>(
                new SimpleStringSchema(), "value", "codec", ignoreDecompressionErrors);
        SourceReaderContext context = mock(SourceReaderContext.class);
        SourceReaderMetricGroup metrics = mock(SourceReaderMetricGroup.class);
        when(context.metricGroup()).thenReturn(metrics);
        when(metrics.counter(anyString())).thenAnswer(ignored -> new SimpleCounter());
        deserializer.open(context);
        YtQueueRecordEmitter<String> emitter = new YtQueueRecordEmitter<>(deserializer);
        YtQueueSplitState state = new YtQueueSplitState(split("queue-id", 0, 7));
        SourceOutput<String> output = output();
        YtQueueRawRecord corruptedRecord = new YtQueueRawRecord(0, 7, corruptedRow, schema);

        if (!ignoreDecompressionErrors) {
            assertThatThrownBy(() -> emitter.emitRecord(corruptedRecord, output, state))
                    .isInstanceOf(IllegalArgumentException.class);
            verify(output, never()).collect(org.mockito.ArgumentMatchers.any());
            assertThat(state.getNextOffset()).isEqualTo(7);
            return;
        }

        emitter.emitRecord(corruptedRecord, output, state);

        verify(output, never()).collect(org.mockito.ArgumentMatchers.any());
        assertThat(state.getNextOffset()).isEqualTo(8);

        UnversionedRow validRow = new UnversionedRow(List.of(
                new UnversionedValue(0, ColumnValueType.STRING, false,
                        "next".getBytes(StandardCharsets.UTF_8))));
        emitter.emitRecord(new YtQueueRawRecord(0, 8, validRow, schema), output, state);

        verify(output).collect("next");
        assertThat(state.getNextOffset()).isEqualTo(9);
    }

    @SuppressWarnings("unchecked")
    private static <T> SourceOutput<T> output() {
        return mock(SourceOutput.class);
    }
}
