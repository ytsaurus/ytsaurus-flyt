package tech.ytsaurus.flyt.connectors.ytsaurus.common.providers.reshard;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalDouble;
import java.util.concurrent.TimeUnit;

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
public class LastPartitionReshardProvider implements ReshardProvider {

    private static final long DEFAULT_TIMEOUT_MS = 10_000L;

    private final PartitionConfig partitionConfig;

    public LastPartitionReshardProvider(PartitionConfig partitionConfig) {
        this.partitionConfig = Preconditions.checkNotNull(partitionConfig, "partitionConfig can't be null");
    }

    @Override
    public ReshardTable get(YTsaurusClient ytClient, ComplexYtPath path, TableSchema schema, ReshardingConfig config) {
        int tabletsCount = getTabletsCount(ytClient, path, config);
        return ReshardTable.builder()
                .setPath(path.getFullPath())
                .setSchema(schema)
                .setUnconvertedPivotKeys(ReshardUtils.getUniformPartitioning(tabletsCount))
                .build();
    }

    private int getTabletsCount(YTsaurusClient ytClient, ComplexYtPath path, ReshardingConfig config) {
        try {
            if (!path.isPartitioned()) {
                log.warn("Yt table path {} is not partitioned. Using default tablets count - {}",
                        path.getFullPath(), config.getTabletCount());
                return config.getTabletCount();
            }
            ListNode getNodeRequest = ListNode.builder()
                    .setPath(YPath.simple(path.getBasePath()))
                    .setAttributes(List.of(YT_TABLET_COUNT_ATTRIBUTE_NAME))
                    .build();
            YTreeNode baseDirNode = ytClient.listNode(getNodeRequest).get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            Preconditions.checkState(baseDirNode.isListNode(), String.format(
                    "Unexpected node type of table base directory '%s' - %s", path.getBasePath(), baseDirNode));

            OptionalDouble avgTabletsCount = baseDirNode.asList().stream()
                    .sorted(Comparator.comparing(this::getPartitionInstant).reversed())
                    .limit(config.getLastPartitionsCount())
                    .flatMap(node -> node.getAttribute(YT_TABLET_COUNT_ATTRIBUTE_NAME).stream())
                    .mapToInt(YTreeNode::intValue)
                    .average();
            return (int) avgTabletsCount.orElse(config.getTabletCount());
        } catch (Exception e) {
            log.error("Failed to calculate tablets count based on last YT table partitions. " +
                    "Using the default value - {}", config.getTabletCount(), e);
            return config.getTabletCount();
        }
    }

    private Instant getPartitionInstant(YTreeNode partitionNode) {
        return partitionConfig.getPartitionScale().getAccessor().toInstant(partitionNode.stringNode().stringValue());
    }
}
