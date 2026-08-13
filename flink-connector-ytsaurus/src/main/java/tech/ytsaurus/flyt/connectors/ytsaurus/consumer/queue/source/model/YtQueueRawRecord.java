package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model;

import java.util.Objects;

import lombok.Getter;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;

@Getter
public final class YtQueueRawRecord {
    private final int partitionIndex;
    private final long offset;
    private final UnversionedRow row;
    private final TableSchema schema;

    public YtQueueRawRecord(int partitionIndex, long offset, UnversionedRow row, TableSchema schema) {
        if (partitionIndex < 0) {
            throw new IllegalArgumentException("partitionIndex must not be negative");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must not be negative");
        }
        this.partitionIndex = partitionIndex;
        this.offset = offset;
        this.row = Objects.requireNonNull(row, "row");
        this.schema = Objects.requireNonNull(schema, "schema");
    }
}
