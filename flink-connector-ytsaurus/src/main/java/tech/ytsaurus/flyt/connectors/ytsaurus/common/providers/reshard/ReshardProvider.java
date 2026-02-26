package tech.ytsaurus.flyt.connectors.ytsaurus.common.providers.reshard;

import java.io.Serializable;

import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.ReshardTable;
import tech.ytsaurus.core.tables.TableSchema;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardingConfig;

public interface ReshardProvider extends Serializable {

    ReshardingConfig getReshardingConfig();

    ReshardTable makeReshardRequest(ComplexYtPath path, TableSchema schema, int tabletCount);

    int calculateTabletCount(YTsaurusClient ytClient, ComplexYtPath path);
}
