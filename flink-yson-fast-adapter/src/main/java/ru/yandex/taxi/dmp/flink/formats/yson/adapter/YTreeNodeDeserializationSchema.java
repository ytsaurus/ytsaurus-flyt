package ru.yandex.taxi.dmp.flink.formats.yson.adapter;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import tech.ytsaurus.ysontree.YTreeNode;

public interface YTreeNodeDeserializationSchema extends DeserializationSchema<RowData> {
    RowData deserialize(YTreeNode node);
}
