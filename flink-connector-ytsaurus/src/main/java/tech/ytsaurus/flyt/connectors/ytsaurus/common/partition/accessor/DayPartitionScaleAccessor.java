package tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class DayPartitionScaleAccessor extends StartDatePartitionScaleAccessor {
    public DayPartitionScaleAccessor() {
        super(ChronoUnit.DAYS);
    }

    @Override
    public LocalDate convertStart(LocalDate localDate) {
        return localDate;
    }
}
