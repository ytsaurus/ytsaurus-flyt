package tech.ytsaurus.flyt.connectors.ytsaurus.common;

import java.util.Map;

import org.junit.jupiter.api.Test;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.junit.jupiter.api.Assertions.assertEquals;

class YtTableAttributesTest {

    @Test
    void getAttributes() {
        YtTableAttributes tableAttributes = YtTableAttributes.empty()
                .setOptimizeFor(OptimizeFor.LOOKUP)
                .setPrimaryMedium(PrimaryMedium.SSD_BLOBS)
                .setCustomAttributes(Map.of(
                        "compression_codec", "zstd_5",
                        "erasure_codec", "lrc_12_2_2"
                    )
                );

        Map<String, YTreeNode> attributes = tableAttributes.getAttributes();

        assertEquals(Map.of("dynamic", YTree.booleanNode(true),
                        "optimize_for", YTree.stringNode("lookup"),
                        "primary_medium", YTree.stringNode("ssd_blobs"),
                        "compression_codec", YTree.stringNode("zstd_5"),
                        "erasure_codec", YTree.stringNode("lrc_12_2_2")),
                attributes);
    }

    @Test
    void getAttributesWithCustomAttributes() {
        YtTableAttributes tableAttributes = YtTableAttributes.empty()
                .setOptimizeFor(OptimizeFor.LOOKUP)
                .setPrimaryMedium(PrimaryMedium.SSD_BLOBS)
                .setCustomAttributes(Map.of(
                        "custom_attribute1", "custom_value1",
                        "custom_attribute2", "custom_value2"
                    )
                );

        Map<String, YTreeNode> attributes = tableAttributes.getAttributes();

        assertEquals(Map.of("dynamic", YTree.booleanNode(true),
                        "optimize_for", YTree.stringNode("lookup"),
                        "primary_medium", YTree.stringNode("ssd_blobs"),
                        "custom_attribute1", YTree.stringNode("custom_value1"),
                        "custom_attribute2", YTree.stringNode("custom_value2")),
                attributes);
    }

    @Test
    void getAttributesWithCustomAttributes_whenOverrideAttribute() {
        YtTableAttributes tableAttributes = YtTableAttributes.empty()
                .setOptimizeFor(OptimizeFor.LOOKUP)
                .setPrimaryMedium(PrimaryMedium.SSD_BLOBS)
                .setCustomAttributes(Map.of(
                        "custom_attribute1", "custom_value1",
                        "custom_attribute2", "custom_value2",
                        "optimize_for", "scan"
                    )
                );

        Map<String, YTreeNode> attributes = tableAttributes.getAttributes();

        assertEquals(Map.of("dynamic", YTree.booleanNode(true),
                        "optimize_for", YTree.stringNode("scan"),
                        "primary_medium", YTree.stringNode("ssd_blobs"),
                        "custom_attribute1", YTree.stringNode("custom_value1"),
                        "custom_attribute2", YTree.stringNode("custom_value2")),
                attributes);
    }

    @Test
    void setCustomAttributes() {
        YtTableAttributes tableAttributes = YtTableAttributes.empty()
                .setOptimizeFor(OptimizeFor.LOOKUP)
                .setPrimaryMedium(PrimaryMedium.SSD_BLOBS)
                .setCustomAttributes(Map.of(
                        "custom_str", "custom_value1",
                        "custom_int", "1",
                        "custom_uint", "1u",
                        "custom_double", "1.1",
                        "custom_boolean", "%true"
                    )
                );

        Map<String, YTreeNode> attributes = tableAttributes.getAttributes();

        assertEquals(Map.of("dynamic", YTree.booleanNode(true),
                        "optimize_for", YTree.stringNode("lookup"),
                        "primary_medium", YTree.stringNode("ssd_blobs"),
                        "custom_str", YTree.stringNode("custom_value1"),
                        "custom_int", YTree.integerNode(1),
                        "custom_uint", YTree.unsignedIntegerNode(1),
                        "custom_double", YTree.doubleNode(1.1),
                        "custom_boolean", YTree.booleanNode(true)),
                attributes);
    }

    @Test
    void setCustomAttributesYson_worksForCompatibility() {
        YtTableAttributes tableAttributes = YtTableAttributes.empty()
                .setOptimizeFor(OptimizeFor.LOOKUP)
                .setPrimaryMedium(PrimaryMedium.SSD_BLOBS)
                .setCustomAttributesYson(
                        "{\n" +
                                "  \"custom_attribute1\" = \"custom_value1\";\n" +
                                "  \"custom_attribute2\" = %true;\n" +
                                "  \"custom_attribute3\" = 1;\n" +
                                "}"
                );

        Map<String, YTreeNode> attributes = tableAttributes.getAttributes();

        assertEquals(Map.of("dynamic", YTree.booleanNode(true),
                        "optimize_for", YTree.stringNode("lookup"),
                        "primary_medium", YTree.stringNode("ssd_blobs"),
                        "custom_attribute1", YTree.stringNode("custom_value1"),
                        "custom_attribute2", YTree.booleanNode(true),
                        "custom_attribute3", YTree.integerNode(1)),
                attributes);
    }
}
