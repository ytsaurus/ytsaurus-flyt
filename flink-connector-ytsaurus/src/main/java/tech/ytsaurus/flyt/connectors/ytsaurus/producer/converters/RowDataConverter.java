package tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters;

import java.io.Serializable;

import org.apache.flink.table.data.RowData;

public interface RowDataConverter<T> extends Serializable {
    /**
     * Convert a value from row data to T value
     *
     * @param rowData        row data
     * @param columnKeyIndex index of the value to convert in row data
     * @return converted temporal
     */
    T convert(RowData rowData, int columnKeyIndex);
}
