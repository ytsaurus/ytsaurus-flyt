package tech.ytsaurus.flyt.connectors.ytsaurus.common.providers.reshard;

import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.ReshardTable;
import tech.ytsaurus.core.tables.TableSchema;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardingConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.ReshardUtils;

public class FixedReshardProvider implements ReshardProvider {

    @Override
    public ReshardTable get(YTsaurusClient ytClient, ComplexYtPath path, TableSchema schema, ReshardingConfig config) {
        return ReshardTable.builder()
                .setPath(path.getFullPath())
                .setSchema(schema)
                .setUnconvertedPivotKeys(ReshardUtils.getUniformPartitioning(config.getTabletCount()))
//                Tablet Count is not needed to be passed if PivotKeys are passed
//                .setTabletCount(config.getTabletCount())
                .build();
    }
}
