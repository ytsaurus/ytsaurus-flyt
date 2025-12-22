package tech.ytsaurus.flyt.connectors.ytsaurus.common;

public enum ReshardStrategy {
    /**
     * Disable resharding
     */
    NONE,
    /**
     * Resharding table with fixed number of tablets
     */
    FIXED,
    /**
     * Resharding table based on the average number of tablets in the last N partitions.
     * If there are no previous partitions, default tablets count will be used (like in FIXED mode).
     */
    LAST_PARTITIONS,
}
