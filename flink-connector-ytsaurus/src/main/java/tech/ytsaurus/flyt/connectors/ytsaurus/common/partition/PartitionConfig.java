package tech.ytsaurus.flyt.connectors.ytsaurus.common.partition;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.InstantRowDataConverter;

@AllArgsConstructor
@Value
@Builder
public class PartitionConfig implements Serializable {
    String partitionKey;
    PartitionScale partitionScale;
    Integer partitionTtlDayCnt;
    Integer partitionTtlInDaysFromCreation;
    Integer partitionMinTtl;
    InstantRowDataConverter converter;

    public PartitionConfig(String partitionKey, PartitionScale partitionScale, InstantRowDataConverter converter) {
        this.partitionKey = partitionKey;
        this.partitionScale = partitionScale;
        this.partitionTtlDayCnt = null;
        this.partitionTtlInDaysFromCreation = null;
        this.partitionMinTtl = null;
        this.converter = converter;
    }
}
