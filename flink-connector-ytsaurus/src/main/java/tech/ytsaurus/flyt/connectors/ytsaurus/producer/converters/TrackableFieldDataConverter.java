package tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters;

import java.util.Map;
import java.util.function.Function;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE;

@Slf4j
public class TrackableFieldDataConverter implements RowDataConverter<Long> {
    private static final Map<LogicalTypeRoot, Function<LogicalType, RowDataConverter<Long>>> CONVERTERS = Map.of(
            TIMESTAMP_WITHOUT_TIME_ZONE,
            type -> timestampConverter(((TimestampType) type).getPrecision()),
            TIMESTAMP_WITH_TIME_ZONE,
            type -> timestampConverter(((ZonedTimestampType) type).getPrecision()),
            TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            type -> timestampConverter(((LocalZonedTimestampType) type).getPrecision()));

    private final RowDataConverter<Long> converter;

    public TrackableFieldDataConverter(LogicalType logicalType) {
        converter = CONVERTERS.get(logicalType.getTypeRoot()).apply(logicalType);
        if (converter == null) {
            throw new IllegalArgumentException(
                    String.format("Unexpected trackable field type. Expected one of %s, but got %s.",
                            CONVERTERS.keySet(), logicalType.getTypeRoot())
            );
        }
    }

    private static RowDataConverter<Long> timestampConverter(int precision) {
        return (rowData, columnKeyIndex) -> {
            TimestampData data = rowData.getTimestamp(columnKeyIndex, precision);
            return data.getMillisecond();
        };
    }

    @Override
    public Long convert(RowData rowData, int columnKeyIndex) {
        return converter.convert(rowData, columnKeyIndex);
    }
}
