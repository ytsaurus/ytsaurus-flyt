package tech.ytsaurus.flyt.connectors.ytsaurus.common;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.ValidationException;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_DYNAMIC_ATTRIBUTE_NAME;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_MIN_TABLET_COUNT;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_OPTIMIZE_FOR_ATTRIBUTE_NAME;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_PRIMARY_MEDIUM_ATTRIBUTE_NAME;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_TABLET_BALANCER_CONFIG;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_TABLET_CELL_BUNDLE;

@Slf4j
@Data
@Accessors(chain = true)
public class YtTableAttributes implements Serializable {
    public static final String CUSTOM_ATTRIBUTES_PREFIX = "attributes.";

    private OptimizeFor optimizeFor;

    private PrimaryMedium primaryMedium;

    private Integer minTabletCount;

    private String tabletCellBundle;

    private Map<String, String> customAttributes;

    public static YtTableAttributes empty() {
        return new YtTableAttributes();
    }

    public Map<String, YTreeNode> getAttributes() {
        Map<String, YTreeNode> attr = new HashMap<>();

        attr.put(YT_DYNAMIC_ATTRIBUTE_NAME, YTree.booleanNode(true));

        if (optimizeFor != null) {
            attr.put(YT_OPTIMIZE_FOR_ATTRIBUTE_NAME, YTree.stringNode(optimizeFor.name().toLowerCase()));
        }
        if (primaryMedium != null) {
            attr.put(YT_PRIMARY_MEDIUM_ATTRIBUTE_NAME, YTree.stringNode(primaryMedium.name().toLowerCase()));
        }
        if (minTabletCount != null) {
            YTreeNode balancerConfig = YTree.mapBuilder()
                    .key(YT_MIN_TABLET_COUNT).value(YTree.integerNode(minTabletCount))
                    .buildMap();
            attr.putIfAbsent(YT_TABLET_BALANCER_CONFIG, balancerConfig);
        }
        if (tabletCellBundle != null) {
            attr.putIfAbsent(YT_TABLET_CELL_BUNDLE, YTree.stringNode(tabletCellBundle));
        }

        // Custom attributes must be processed last because:
        // They can override other YT attributes (e.g., optimize_for, tablet_count)
        if (customAttributes != null) {
            customAttributes.forEach((attrName, attrValue) -> {
                if (attr.containsKey(attrName)) {
                    log.warn("Overwriting YT table attribute '{}' (old: '{}', new: '{}')",
                            attrName, attr.get(attrName).stringValue(), attrValue);
                }
                attr.put(attrName, YTreeTextSerializer.deserialize(attrValue));
            });
        }
        return attr;
    }

    /**
     * @deprecated use {@link #setCustomAttributes(Map)}
     */
    @Deprecated
    public YtTableAttributes setCustomAttributesYson(String customAttributesYson) {
        YTreeNode customAttributesYsonNode = YTreeTextSerializer.deserialize(customAttributesYson);
        if (!customAttributesYsonNode.isMapNode()) {
            throw new ValidationException("Custom attributes must be a map");
        }
        this.customAttributes = customAttributesYsonNode.mapNode().asMap().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> YTreeTextSerializer.serialize(entry.getValue())));
        return this;
    }
}
