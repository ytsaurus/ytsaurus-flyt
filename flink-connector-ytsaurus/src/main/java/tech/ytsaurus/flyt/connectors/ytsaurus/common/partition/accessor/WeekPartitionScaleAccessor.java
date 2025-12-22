package tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class WeekPartitionScaleAccessor extends StartDatePartitionScaleAccessor {
    public WeekPartitionScaleAccessor() {
        super(ChronoUnit.WEEKS);
    }

    @Override
    public LocalDate convertStart(LocalDate localDate) {
        return localDate.minusDays(localDate.getDayOfWeek().getValue() - 1);
    }
}
