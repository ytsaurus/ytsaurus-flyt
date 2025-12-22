package tech.ytsaurus.flyt.connectors.ytsaurus.common.partition;

import java.time.Instant;
import java.time.temporal.Temporal;

public interface PartitionScaleAccessor {
    /**
     * Get start of the partition range picked for raw instant.
     * <p>
     * For example, if current partitioning is by hour, then
     * raw=2024-04-15 12:27:00 falls in range start=2024-04-15 12:00:00 <= raw < end=2024-04-15 13:00:00
     *
     * @param raw raw instant to get partition end for
     * @return end of partition for specified raw instant
     */
    Temporal getStart(Instant raw);

    /**
     * Get end of the partition range picked for raw instant.
     * <p>
     * For example, if current partitioning is by hour, then
     * raw=2024-04-15 12:27:00 falls in range start=2024-04-15 12:00:00 <= raw < end=2024-04-15 13:00:00
     *
     * @param raw raw instant to get partition end for
     * @return end of partition for specified raw instant
     */
    Temporal getEnd(Instant raw);

    /**
     * Format instant to represent the smallest partition unit's name.
     * For example, daily partition scale can format the current instant to yyyy-MM-dd,
     * which represents a unique day.
     *
     * @param instant to format
     * @return string that represents the smallest partition unit
     * @implNote This method is used to determine the name of partitions' tables
     */
    String getName(Instant instant);

    Instant toInstant(String partitionValue);
}
