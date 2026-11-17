package tech.ytsaurus.flyt.connectors.ytsaurus.producer.queue;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.typeinfo.TiType;

final class YtQueueSinkSchemaValidator {
    private YtQueueSinkSchemaValidator() {
    }

    static void validate(String queuePath, LogicalType flinkType, ColumnSchema column) {
        if (!isCompatible(flinkType, column.getTypeV3())) {
            throw new IllegalArgumentException("Queue '" + queuePath + "' column '" + column.getName() +
                    "': Flink type " + flinkType.asSummaryString() + " is incompatible with YT type " +
                    column.getTypeV3() + " in ROW mode");
        }
    }

    private static boolean isCompatible(LogicalType flinkType, TiType ytType) {
        switch (flinkType.getTypeRoot()) {
            case BINARY:
            case VARBINARY:
                // BYTES contains embedded YSON; its type can only be checked from the value.
                return true;
            default:
                break;
        }
        ytType = unwrapTagged(ytType);
        if (flinkType.isNullable() && !ytType.isNullable()) {
            return false;
        }
        if (flinkType.getTypeRoot() == LogicalTypeRoot.NULL) {
            return ytType.isNullable();
        }
        if (ytType.isOptional()) {
            ytType = unwrapTagged(ytType.asOptional().getItem());
        }
        if (ytType.isYson()) {
            return true;
        }
        switch (flinkType.getTypeRoot()) {
            case BOOLEAN:
                return ytType.isBool();
            case TINYINT:
                return signedIntegerWidth(ytType) >= Byte.SIZE;
            case SMALLINT:
                return signedIntegerWidth(ytType) >= Short.SIZE;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return signedIntegerWidth(ytType) >= Integer.SIZE;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return ytType.isInt64();
            case FLOAT:
                return ytType.isFloat() || ytType.isDouble();
            case DOUBLE:
                return ytType.isDouble();
            case CHAR:
            case VARCHAR:
                return ytType.isString() || ytType.isUtf8() || ytType.isJson();
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ytType.isString() || ytType.isUtf8();
            case ARRAY:
                return ytType.isList() && isCompatible(
                        ((ArrayType) flinkType).getElementType(), ytType.asList().getItem());
            default:
                return false;
        }
    }

    private static TiType unwrapTagged(TiType type) {
        while (type.isTagged()) {
            type = type.asTaggedType().getItem();
        }
        return type;
    }

    private static int signedIntegerWidth(TiType type) {
        switch (type.getTypeName()) {
            case Int8:
                return Byte.SIZE;
            case Int16:
                return Short.SIZE;
            case Int32:
                return Integer.SIZE;
            case Int64:
                return Long.SIZE;
            default:
                return 0;
        }
    }
}
