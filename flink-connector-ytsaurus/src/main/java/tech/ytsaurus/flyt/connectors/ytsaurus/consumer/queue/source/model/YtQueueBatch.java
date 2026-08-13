package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.model;

import java.util.List;
import java.util.Objects;

import lombok.Getter;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;

@Getter
public final class YtQueueBatch {
    private final TableSchema schema;
    private final long startOffset;
    private final List<UnversionedRow> rows;

    public YtQueueBatch(TableSchema schema, long startOffset, List<UnversionedRow> rows) {
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset must not be negative");
        }
        this.schema = Objects.requireNonNull(schema, "schema");
        this.startOffset = startOffset;
        this.rows = Objects.requireNonNull(rows, "rows");
    }

    public long getFinishOffset() {
        return startOffset + rows.size();
    }
}
