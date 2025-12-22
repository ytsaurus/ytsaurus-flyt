package tech.ytsaurus.flyt.connectors.ytsaurus.common.providers.reshard;

import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.ReshardTable;
import tech.ytsaurus.core.tables.TableSchema;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardingConfig;

public interface ReshardProvider {
    ReshardTable get(YTsaurusClient ytClient, ComplexYtPath path, TableSchema schema, ReshardingConfig config);
}
