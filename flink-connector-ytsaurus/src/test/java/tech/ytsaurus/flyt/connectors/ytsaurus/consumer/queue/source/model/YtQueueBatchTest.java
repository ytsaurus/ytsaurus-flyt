package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model;

import java.util.List;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class YtQueueBatchTest {
    @Test
    void derivesFinishOffsetFromStartOffsetAndRowCount() {
        YtQueueBatch batch = new YtQueueBatch(
                TableSchema.builder().build(),
                10,
                List.of(row(), row(), row()));

        assertThat(batch.getStartOffset()).isEqualTo(10);
        assertThat(batch.getFinishOffset()).isEqualTo(13);
        assertThat(batch.getRows()).hasSize(3);
    }

    @Test
    void emptyBatchKeepsStartOffsetAsFinishOffset() {
        YtQueueBatch batch = new YtQueueBatch(
                TableSchema.builder().build(),
                42,
                List.of());

        assertThat(batch.getFinishOffset()).isEqualTo(42);
    }

    @Test
    void rejectsNegativeStartOffset() {
        assertThatThrownBy(() -> new YtQueueBatch(
                TableSchema.builder().build(),
                -1,
                List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("startOffset must not be negative");
    }

    private static UnversionedRow row() {
        return new UnversionedRow(List.of());
    }
}
