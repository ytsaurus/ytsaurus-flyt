package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.experimental.UtilityClass;
import tech.ytsaurus.typeinfo.TypeName;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

@UtilityClass
public class ConverterUtils {
    public static final String SCHEMA_TYPE_NAME = "type";
    public static final String DATE_YT_TYPE = TypeName.Date.getWireName();
    public static final String DATETIME_YT_TYPE = TypeName.Datetime.getWireName();
    public static final String TIMESTAMP_YT_TYPE = TypeName.Timestamp.getWireName();
    public static final String INTERVAL_YT_TYPE = TypeName.Interval.getWireName();

    public static final Set<String> NATIVE_YT_TYPES = Set.of(
            DATE_YT_TYPE,
            DATETIME_YT_TYPE,
            TIMESTAMP_YT_TYPE,
            INTERVAL_YT_TYPE);

    /**
     * Check if current field node is of any native type (chrono)
     *
     * @param fieldNode YT schema field YSON node
     * @return if current field is of any chrono native types
     * @see ConverterUtils#NATIVE_YT_TYPES
     */
    public static boolean isOfNativeType(YTreeNode fieldNode) {
        return Optional.ofNullable(fieldNode)
                .map(ConverterUtils::getType)
                .map(NATIVE_YT_TYPES::contains)
                .orElse(false);
    }

    /**
     * Get type from current field YSON node
     *
     * @param fieldNode YT schema field YSON node
     * @return null if type cannot be inferred and YT type if exists otherwise
     * @see <a href="https://ytsaurus.tech/docs/en/user-guide/storage/data-types">YT types</a>
     */
    public static String getType(YTreeNode fieldNode) {
        if (!fieldNode.isMapNode()) {
            return null;
        }
        return fieldNode.mapNode()
                .getStringO(SCHEMA_TYPE_NAME)
                .orElse(null);
    }

    public static YTreeNode toWriteNode(YTreeNode fieldNode) {
        List<YTreeNode> writeColumns = fieldNode.asList().stream()
                .filter(columnNode -> columnNode.asMap().get("expression") == null)
                .collect(Collectors.toList());

        YTreeBuilder nodeBuilder = YTree.builder()
                .beginAttributes();
        fieldNode.getAttributes().forEach((key, value) -> nodeBuilder.key(key).value(value));
        nodeBuilder.endAttributes()
                .value(writeColumns);
        return nodeBuilder.build();
    }
}
