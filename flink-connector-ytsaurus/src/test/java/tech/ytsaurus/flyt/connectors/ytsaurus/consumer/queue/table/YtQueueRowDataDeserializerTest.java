package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.UserCodeClassLoader;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeMapNode;

import tech.ytsaurus.flyt.formats.yson.adapter.YTreeNodeDeserializationSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class YtQueueRowDataDeserializerTest {
    @Test
    void usesYTreeNodeFastPath() throws Exception {
        UnversionedRow row = mock(UnversionedRow.class);
        TableSchema tableSchema = mock(TableSchema.class);
        YTreeMapNode node = mock(YTreeMapNode.class);
        YTreeNodeDeserializationSchema schema = mock(YTreeNodeDeserializationSchema.class);
        RowData expected = GenericRowData.of("value");
        when(row.toYTreeMap(tableSchema, true)).thenReturn(node);
        when(schema.deserialize(node)).thenReturn(expected);

        RowData actual = new YtQueueRowDataDeserializer(schema).deserialize(row, tableSchema);

        assertThat(actual).isSameAs(expected);
        verify(node, never()).toBinary();
    }

    @Test
    void serializesNodeForGenericFlinkFormat() throws Exception {
        UnversionedRow row = mock(UnversionedRow.class);
        TableSchema tableSchema = mock(TableSchema.class);
        YTreeMapNode node = mock(YTreeMapNode.class);
        @SuppressWarnings("unchecked")
        DeserializationSchema<RowData> schema = mock(DeserializationSchema.class);
        RowData expected = GenericRowData.of("value");
        byte[] binaryYson = new byte[] {1, 2, 3};
        when(row.toYTreeMap(tableSchema, true)).thenReturn(node);
        when(node.toBinary()).thenReturn(binaryYson);
        when(schema.deserialize(binaryYson)).thenReturn(expected);

        RowData actual = new YtQueueRowDataDeserializer(schema).deserialize(row, tableSchema);

        assertThat(actual).isSameAs(expected);
    }

    @Test
    void opensFlinkDeserializerWithSourceReaderContext() throws Exception {
        YTreeNodeDeserializationSchema schema = mock(YTreeNodeDeserializationSchema.class);
        SourceReaderContext context = mock(SourceReaderContext.class);
        SourceReaderMetricGroup metricGroup = mock(SourceReaderMetricGroup.class);
        UserCodeClassLoader classLoader = mock(UserCodeClassLoader.class);
        when(context.metricGroup()).thenReturn(metricGroup);
        when(context.getUserCodeClassLoader()).thenReturn(classLoader);

        new YtQueueRowDataDeserializer(schema).open(context);

        ArgumentCaptor<DeserializationSchema.InitializationContext> initializationContext =
                ArgumentCaptor.forClass(DeserializationSchema.InitializationContext.class);
        verify(schema).open(initializationContext.capture());
        assertThat(initializationContext.getValue().getMetricGroup()).isSameAs(metricGroup);
        assertThat(initializationContext.getValue().getUserCodeClassLoader()).isSameAs(classLoader);
    }
}
