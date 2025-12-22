package tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static tech.ytsaurus.flyt.connectors.ytsaurus.utils.PartitionScaleUtils.DEFAULT_ZONE_OFFSET;

public class HourPartitionScaleAccessor extends StartDateTimePartitionScaleAccessor {

    private static final DateTimeFormatter PYTHON_LOCAL_DATE_TIME_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(ISO_LOCAL_TIME)
                    .toFormatter();

    public HourPartitionScaleAccessor() {
        super(ChronoUnit.HOURS);
    }

    @Override
    public LocalDateTime convertStart(LocalDateTime localDateTime) {
        return localDateTime
                .minusMinutes(localDateTime.getMinute())
                .minusSeconds(localDateTime.getSecond())
                .minusNanos(localDateTime.getNano());
    }

    @Override
    public String format(LocalDateTime localDateTime) {
        return PYTHON_LOCAL_DATE_TIME_FORMATTER.format(localDateTime);
    }

    @Override
    public Instant toInstant(String partitionValue) {
        return LocalDateTime.parse(partitionValue, PYTHON_LOCAL_DATE_TIME_FORMATTER).toInstant(DEFAULT_ZONE_OFFSET);
    }
}
