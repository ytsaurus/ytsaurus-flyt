package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.ysontree.YTreeMapNode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class YtStaticRowSerializerTest {
    @Test
    void detachesMutableRowAndBinaryBuffersAndSurvivesJobSerialization() throws Exception {
        RowType rowType = (RowType) DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.BIGINT()),
                DataTypes.FIELD("text", DataTypes.STRING()),
                DataTypes.FIELD("payload", DataTypes.BYTES())).getLogicalType();
        String schema = "[{name=id;type=int64};{name=text;type=string};{name=payload;type=string}]";
        YtStaticRowSerializer<RowData> serializer = YtStaticRowSerializer.forRowData(
                rowType, schema, TimestampFormat.ISO_8601);
        byte[] input = {1, 2, 3};
        GenericRowData row = GenericRowData.of(1L, StringData.fromString("before"), input);
        YTreeMapNode converted = serializer.serialize(row);
        row.setField(0, 2L);
        input[0] = 100;
        assertThat(converted.getLong("id")).isEqualTo(1);
        assertThat(converted.getBytes("payload")).containsExactly(1, 2, 3);

        YtStaticRowSerializer<RowData> restored = InstantiationUtil.deserializeObject(
                InstantiationUtil.serializeObject(serializer), getClass().getClassLoader());
        assertThat(restored.serialize(row).getLong("id")).isEqualTo(2);
    }

    @Test
    void preservesExistingNativeTimestampMapping() {
        RowType rowType = (RowType) DataTypes.ROW(DataTypes.FIELD("ts", DataTypes.TIMESTAMP(6))).getLogicalType();
        YtStaticRowSerializer<RowData> serializer = YtStaticRowSerializer.forRowData(
                rowType, "[{name=ts;type=timestamp}]", TimestampFormat.ISO_8601);
        assertThat(serializer.serialize(GenericRowData.of(TimestampData.fromEpochMillis(123))).getLong("ts"))
                .isEqualTo(123000);
    }

    @Test
    void rejectsEveryNonInsertRowKind() {
        RowType type = (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();
        YtStaticRowSerializer<RowData> serializer = YtStaticRowSerializer.forRowData(
                type, YtStaticSinkWriterTest.SCHEMA, TimestampFormat.ISO_8601);
        for (RowKind kind : new RowKind[]{RowKind.DELETE, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER}) {
            GenericRowData row = GenericRowData.of(1L);
            row.setRowKind(kind);
            assertThrows(IllegalArgumentException.class, () -> serializer.serialize(row));
        }
    }
}
