package tech.ytsaurus.flyt.connectors.ytsaurus.common.providers.reshard;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.util.Preconditions;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.ListNode;
import tech.ytsaurus.client.request.ReshardTable;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeNode;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardingConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.ReshardUtils;

import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_TABLET_COUNT_ATTRIBUTE_NAME;

@Slf4j
public class LastPartitionsReshardProvider implements ReshardProvider {

    private static final long DEFAULT_TIMEOUT_MS = 10_000L;

    private final ReshardingConfig reshardingConfig;
    private final PartitionConfig partitionConfig;

    public LastPartitionsReshardProvider(ReshardingConfig reshardingConfig, PartitionConfig partitionConfig) {
        this.reshardingConfig = Preconditions.checkNotNull(reshardingConfig, "reshardingConfig can't be null");
        this.partitionConfig = Preconditions.checkNotNull(partitionConfig, "partitionConfig can't be null");
    }

    @Override
    public ReshardingConfig getReshardingConfig() {
        return reshardingConfig;
    }

    @Override
    public ReshardTable makeReshardRequest(ComplexYtPath path, TableSchema schema, int tabletCount) {
        return ReshardTable.builder()
                .setPath(path.getFullPath())
                .setSchema(schema)
                .setUnconvertedPivotKeys(ReshardUtils.getUniformPartitioning(tabletCount))
                .build();
    }

    @Override
    public int calculateTabletCount(YTsaurusClient ytClient, ComplexYtPath path) {
        try {
            if (!path.isPartitioned()) {
                log.warn("Yt table path {} is not partitioned. Using default tablets count - {}",
                        path.getFullPath(), reshardingConfig.getTabletCount());
                return reshardingConfig.getTabletCount();
            }
            ListNode getNodeRequest = ListNode.builder()
                    .setPath(YPath.simple(path.getBasePath()))
                    .setAttributes(List.of(YT_TABLET_COUNT_ATTRIBUTE_NAME))
                    .build();
            YTreeNode baseDirNode = ytClient.listNode(getNodeRequest).get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            Preconditions.checkState(baseDirNode.isListNode(), String.format(
                    "Unexpected node type of YT table base directory '%s' - %s", path.getBasePath(), baseDirNode));

            Map<String, Integer> lastPartitionsTablets = baseDirNode.asList().stream()
                    .sorted(Comparator.comparing(this::getPartitionInstant).reversed())
                    .limit(reshardingConfig.getLastPartitionsCount())
                    .collect(Collectors.toMap(YTreeNode::stringValue, node -> getPartitionTabletsCount(node, path)));

            log.info("Got latest {} partition tablets count for YT table {}: {}",
                    reshardingConfig.getLastPartitionsCount(), path.getBasePath(), lastPartitionsTablets);

            OptionalDouble avgTabletsCount = lastPartitionsTablets.entrySet().stream()
                    // ignore the same partition (if it is already exists) and partitions without tablet_count attribute
                    .filter(e -> !e.getKey().equals(path.getTableName()) && e.getValue() > 0)
                    .mapToInt(Map.Entry::getValue)
                    .average();
            if (avgTabletsCount.isPresent()) {
                int tabletsCount = (int) avgTabletsCount.getAsDouble();
                log.info("Calculated {} tablets count for new YT table partition '{}'", tabletsCount, path.getFullPath());
                return tabletsCount;
            }
            log.warn("Couldn't find existing partitions with '{}' attribute for YT table '{}'. " +
                            "Using default tablet count {} for new partition {}.", YT_TABLET_COUNT_ATTRIBUTE_NAME,
                    path.getBasePath(), reshardingConfig.getTabletCount(), path.getTableName());
            return reshardingConfig.getTabletCount();
        } catch (Exception e) {
            log.error("Failed to calculate tablets count based on last YT table partitions. Using " +
                    "the default value {} for table {}", reshardingConfig.getTabletCount(), path.getFullPath(), e);
            return reshardingConfig.getTabletCount();
        }
    }

    private Integer getPartitionTabletsCount(YTreeNode partitionNode, ComplexYtPath path) {
        Optional<Integer> tabletCount =
                partitionNode.getAttribute(YT_TABLET_COUNT_ATTRIBUTE_NAME).map(YTreeNode::intValue);
        if (tabletCount.isEmpty()) {
            log.warn("There are no '{}' attribute in the partition '{}' of YT table '{}'. Ignoring this partition.",
                    YT_TABLET_COUNT_ATTRIBUTE_NAME, partitionNode.stringValue(), path.getBasePath());
        }
        return tabletCount.orElse(0);
    }

    private Instant getPartitionInstant(YTreeNode partitionNode) {
        return partitionConfig.getPartitionScale().getAccessor().toInstant(partitionNode.stringNode().stringValue());
    }
}
