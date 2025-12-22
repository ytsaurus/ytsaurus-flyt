package tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class YearPartitionScaleAccessor extends StartDatePartitionScaleAccessor {
    public YearPartitionScaleAccessor() {
        super(ChronoUnit.YEARS);
    }

    @Override
    public LocalDate convertStart(LocalDate localDate) {
        return localDate.minusDays(localDate.getDayOfYear() - 1);
    }
}
