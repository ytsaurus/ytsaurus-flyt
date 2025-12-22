package tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;

public class YtPartitioningInstantRowDataConverter implements InstantRowDataConverter {
    private static final long serialVersionUID = 1L;
    private static final long MILLS_IN_SECOND = 1000L;
    private final LogicalType logicalType;

    /**
     * Create partitioning row data converter
     *
     * @param logicalType of the partition key
     */
    public YtPartitioningInstantRowDataConverter(LogicalType logicalType) {
        this.logicalType = logicalType;
    }

    @Override
    public Instant convert(RowData rowData, int columnKeyIndex) {
        RowData.FieldGetter getter = RowData.createFieldGetter(logicalType, columnKeyIndex);
        Object rawValue = getter.getFieldOrNull(rowData);
        if (rawValue == null) {
            throw new IllegalStateException(String.format(
                    "No value present for column index '%d'. Expected one for the logical type %s (preview: %s)",
                    columnKeyIndex, logicalType, rowData.getRawValue(columnKeyIndex)));
        }

        switch (logicalType.getTypeRoot()) {
            case DATE:
                return LocalDate.ofEpochDay((int) rawValue)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant();
            case TIME_WITHOUT_TIME_ZONE:
                return LocalTime.ofSecondOfDay((int) rawValue / MILLS_IN_SECOND)
                        .atDate(LocalDate.MIN)
                        .toInstant(ZoneOffset.UTC);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ((TimestampData) rawValue).toInstant();
            default:
                throw new IllegalStateException(String.format(
                        "Illegal logical type given to convert value of to temporal: %s",
                        logicalType.getTypeRoot().name()));
        }
    }
}
