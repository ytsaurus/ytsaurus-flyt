package tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class MonthPartitionScaleAccessor extends StartDatePartitionScaleAccessor {
    public MonthPartitionScaleAccessor() {
        super(ChronoUnit.MONTHS);
    }

    @Override
    public LocalDate convertStart(LocalDate localDate) {
        return localDate.minusDays(localDate.getDayOfMonth() - 1);
    }
}
