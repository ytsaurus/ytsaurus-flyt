package tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionScaleAccessor;

import static tech.ytsaurus.flyt.connectors.ytsaurus.utils.PartitionScaleUtils.DEFAULT_ZONE_OFFSET;
import static tech.ytsaurus.flyt.connectors.ytsaurus.utils.PartitionScaleUtils.getLocalDate;

public abstract class StartDatePartitionScaleAccessor implements PartitionScaleAccessor, Serializable {

    private static final DateTimeFormatter DEFAULT_LOCAL_DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    private final ChronoUnit unit;

    public StartDatePartitionScaleAccessor(ChronoUnit unit) {
        this.unit = unit;
    }

    public abstract LocalDate convertStart(LocalDate localDate);

    public String format(LocalDate localDate) {
        return DEFAULT_LOCAL_DATE_FORMATTER.format(localDate);
    }

    @Override
    public Instant toInstant(String partitionValue) {
        return LocalDate.parse(partitionValue, DEFAULT_LOCAL_DATE_FORMATTER)
                .atStartOfDay()
                .toInstant(DEFAULT_ZONE_OFFSET);
    }

    @Override
    public LocalDate getStart(Instant raw) {
        return convertStart(getLocalDate(raw));
    }

    @Override
    public LocalDate getEnd(Instant raw) {
        return getStart(raw).plus(1, unit);
    }

    @Override
    public String getName(Instant instant) {
        return format(getStart(instant));
    }
}
