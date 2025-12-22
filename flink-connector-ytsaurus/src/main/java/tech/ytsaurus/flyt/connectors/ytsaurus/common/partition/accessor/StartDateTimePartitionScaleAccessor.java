package tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionScaleAccessor;

import static tech.ytsaurus.flyt.connectors.ytsaurus.utils.PartitionScaleUtils.getLocalDateTime;

public abstract class StartDateTimePartitionScaleAccessor implements PartitionScaleAccessor, Serializable {
    private final ChronoUnit unit;

    public StartDateTimePartitionScaleAccessor(ChronoUnit unit) {
        this.unit = unit;
    }

    public abstract LocalDateTime convertStart(LocalDateTime localDateTime);

    public abstract String format(LocalDateTime localDateTime);

    @Override
    public LocalDateTime getStart(Instant raw) {
        return convertStart(getLocalDateTime(raw));
    }

    @Override
    public LocalDateTime getEnd(Instant raw) {
        return getStart(raw).plus(1, unit);
    }

    @Override
    public String getName(Instant instant) {
        return format(getStart(instant));
    }
}
