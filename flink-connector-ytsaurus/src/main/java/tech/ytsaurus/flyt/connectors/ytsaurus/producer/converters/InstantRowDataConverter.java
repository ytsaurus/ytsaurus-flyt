package tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters;

import java.time.Instant;

import org.apache.flink.table.data.RowData;

public interface InstantRowDataConverter extends RowDataConverter<Instant> {
    /**
     * Convert a value from row data to instant
     *
     * @param rowData        row data
     * @param columnKeyIndex index of the value to convert in row data
     * @return converted temporal
     */
    Instant convert(RowData rowData, int columnKeyIndex);
}
