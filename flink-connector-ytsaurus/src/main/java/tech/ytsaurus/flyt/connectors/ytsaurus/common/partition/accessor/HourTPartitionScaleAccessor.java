package tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static tech.ytsaurus.flyt.connectors.ytsaurus.utils.PartitionScaleUtils.DEFAULT_ZONE_OFFSET;

public class HourTPartitionScaleAccessor extends HourPartitionScaleAccessor {
    private static final DateTimeFormatter DEFAULT_LOCAL_DATE_TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    @Override
    public String format(LocalDateTime localDateTime) {
        return DEFAULT_LOCAL_DATE_TIME_FORMATTER.format(localDateTime);
    }

    @Override
    public Instant toInstant(String partitionValue) {
        return LocalDateTime.parse(partitionValue, DEFAULT_LOCAL_DATE_TIME_FORMATTER).toInstant(DEFAULT_ZONE_OFFSET);
    }
}
