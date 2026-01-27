package tech.ytsaurus.flyt.formats.yson;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import tech.ytsaurus.flyt.formats.yson.adapter.YTreeNodeDeserializationSchema;

import static java.lang.String.format;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class YsonRowDataDeserializationSchema implements YTreeNodeDeserializationSchema {

    private final boolean failOnMissingField;

    private final boolean ignoreParseErrors;

    private final TimestampFormat timestampFormat;

    private final TypeInformation<RowData> resultTypeInfo;

    private final YsonToRowDataConverters.YsonToRowDataConverter runtimeConverter;

    public YsonRowDataDeserializationSchema(RowType rowType,
                                            TypeInformation<RowData> resultTypeInfo,
                                            boolean failOnMissingField,
                                            boolean ignoreParseErrors,
                                            TimestampFormat timestampFormat
    ) {
        if (ignoreParseErrors && failOnMissingField) {
            throw new IllegalArgumentException(
                    "YSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }
        this.resultTypeInfo = checkNotNull(resultTypeInfo);
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = timestampFormat;
        this.runtimeConverter =
                new YsonToRowDataConverters(failOnMissingField, ignoreParseErrors, timestampFormat)
                        .createConverter(checkNotNull(rowType));
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        try {
            return convertToRowData(deserializeToYsonNode(message));
        } catch (Throwable t) {
            throw new IOException(
                    format("Failed to deserialize YSON '%s'.", new String(message)), t);
        }
    }

    public RowData deserialize(YTreeNode node)  {
        if (node == null) {
            return null;
        }
        return convertToRowData(node);
    }

    public YTreeNode deserializeToYsonNode(byte[] message) throws IOException {
        return YTreeTextSerializer.deserialize(new ByteArrayInputStream(message));
    }

    public RowData convertToRowData(YTreeNode message) {
        return (RowData) runtimeConverter.convert(message);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }
}
