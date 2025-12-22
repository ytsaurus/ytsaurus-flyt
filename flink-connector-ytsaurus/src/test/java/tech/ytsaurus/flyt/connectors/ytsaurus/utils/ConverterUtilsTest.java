package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConverterUtilsTest {

    @Test
    void toWriteNode() {
        String schemaToCreate = TestResourceUtils.readResource("order_table_create_schema.txt");
        String schemaToWrite = TestResourceUtils.readResource("order_table_write_schema.txt");

        YTreeNode schemaToCreateNode = YTreeTextSerializer.deserialize(schemaToCreate);
        YTreeNode schemaToWriteNode = YTreeTextSerializer.deserialize(schemaToWrite);

        assertEquals(ConverterUtils.toWriteNode(schemaToCreateNode), schemaToWriteNode);
    }
}
