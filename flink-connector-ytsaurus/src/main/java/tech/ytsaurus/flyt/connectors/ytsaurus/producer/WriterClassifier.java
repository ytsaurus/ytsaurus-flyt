package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Instant;

import lombok.Value;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionConfig;

@Value
public class WriterClassifier {
    String tableName;
    Instant rowDataInstant;
    PartitionConfig partitionConfig;

    public static WriterClassifier plain(String tableName) {
        return new WriterClassifier(tableName, null, null);
    }

    public static WriterClassifier partition(Instant rowDataInstant,
                                             PartitionConfig partitionConfig) {
        return new WriterClassifier(
                partitionConfig
                        .getPartitionScale()
                        .getAccessor()
                        .getName(rowDataInstant),
                rowDataInstant,
                partitionConfig);
    }
}
