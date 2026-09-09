package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;

import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model.YtQueueRawRecord;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplitState;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
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

    @SuppressWarnings("unchecked")
    private static <T> SourceOutput<T> output() {
        return mock(SourceOutput.class);
    }
}
