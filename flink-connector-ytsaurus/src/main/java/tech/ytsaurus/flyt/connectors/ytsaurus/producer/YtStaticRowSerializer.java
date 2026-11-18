package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.io.Serializable;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.ConverterUtils;

/** Converts an input record to a YT row. The returned row must not share mutable input buffers. */
@FunctionalInterface
public interface YtStaticRowSerializer<T> extends Serializable {
    YTreeMapNode serialize(T value);

    static YtStaticRowSerializer<RowData> forRowData(
            RowType rowType, String ysonSchema, TimestampFormat timestampFormat) {
        return new RowDataConverter(rowType, ysonSchema, timestampFormat);
    }

    /** Keeps Flink serializer and generated converters on the TaskManager, not in the job graph. */
    final class RowDataConverter implements YtStaticRowSerializer<RowData> {
        private static final long serialVersionUID = 1L;
        private final RowType rowType;
        private final String schema;
        private final TimestampFormat timestampFormat;
        private transient RowDataSerializer copier;
        private transient RowDataToYtListConverters.RowDataToYtMapConverter converter;

        private RowDataConverter(RowType rowType, String schema, TimestampFormat timestampFormat) {
            this.rowType = rowType;
            this.schema = schema;
            this.timestampFormat = timestampFormat;
        }

        @Override
        public YTreeMapNode serialize(RowData value) {
            if (value.getRowKind() != RowKind.INSERT) {
                throw new IllegalArgumentException("Static YT tables only accept INSERT records");
            }
            if (converter == null) {
                copier = new RowDataSerializer(rowType);
                converter = new RowDataToYtListConverters(timestampFormat).createConverter(
                        rowType, ConverterUtils.toWriteNode(YTreeTextSerializer.deserialize(schema)));
            }
            // Flink may reuse binary/nested RowData immediately after write returns.
            return YTree.builder().value(converter.convert(null, copier.copy(value))).build().mapNode();
        }
    }
}
