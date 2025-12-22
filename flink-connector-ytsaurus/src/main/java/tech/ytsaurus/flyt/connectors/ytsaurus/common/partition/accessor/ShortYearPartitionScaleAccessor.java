package tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static tech.ytsaurus.flyt.connectors.ytsaurus.utils.PartitionScaleUtils.DEFAULT_ZONE_OFFSET;

public class ShortYearPartitionScaleAccessor extends YearPartitionScaleAccessor {

    private static final DateTimeFormatter SHORT_YEAR_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy");

    @Override
    public String format(LocalDate localDate) {
        return SHORT_YEAR_DATE_TIME_FORMATTER.format(localDate);
    }

    @Override
    public Instant toInstant(String partitionValue) {
        return LocalDate.parse(partitionValue, SHORT_YEAR_DATE_TIME_FORMATTER)
                .atStartOfDay()
                .toInstant(DEFAULT_ZONE_OFFSET);
    }
}
