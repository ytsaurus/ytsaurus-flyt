package tech.ytsaurus.flyt.connectors.ytsaurus.common.providers.reshard;

import org.apache.flink.util.Preconditions;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.ReshardTable;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardingConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.ReshardUtils;

public class FixedReshardProvider implements ReshardProvider {

    private final ReshardingConfig reshardingConfig;

    public FixedReshardProvider(ReshardingConfig reshardingConfig) {
        this.reshardingConfig = Preconditions.checkNotNull(reshardingConfig, "reshardingConfig can't be null");
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
//                Tablet Count is not needed to be passed if PivotKeys are passed
//                .setTabletCount(config.getTabletCount())
                .build();
    }

    @Override
    public int calculateTabletCount(YTsaurusClient ytClient, ComplexYtPath path) {
        return reshardingConfig.getTabletCount();
    }
}
