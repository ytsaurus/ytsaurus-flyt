package tech.ytsaurus.flyt.connectors.ytsaurus.common;

import java.io.Serializable;

import lombok.Builder;
import lombok.Data;
import org.apache.flink.util.Preconditions;

import static tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardStrategy.FIXED;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardStrategy.LAST_PARTITIONS;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.ReshardStrategy.NONE;

@Data
@Builder
public class ReshardingConfig implements Serializable {

    private final ReshardStrategy reshardStrategy;
    private final boolean uniform;
    private final int tabletCount;
    private final int lastPartitionsCount;

    public ReshardingConfig(ReshardStrategy reshardStrategy, boolean uniform, int tabletCount,
                            int lastPartitionsCount) {
        Preconditions.checkArgument(reshardStrategy == NONE
                || reshardStrategy == FIXED && tabletCount > 0
                || reshardStrategy == LAST_PARTITIONS && tabletCount > 0 && lastPartitionsCount > 0);
        this.reshardStrategy = reshardStrategy;
        this.uniform = uniform;
        this.tabletCount = tabletCount;
        this.lastPartitionsCount = lastPartitionsCount;
    }

    public static ReshardingConfig none() {
        return ReshardingConfig.builder()
                .reshardStrategy(NONE)
                .build();
    }
}
