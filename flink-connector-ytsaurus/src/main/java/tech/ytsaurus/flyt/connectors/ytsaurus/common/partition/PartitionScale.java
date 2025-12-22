package tech.ytsaurus.flyt.connectors.ytsaurus.common.partition;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor.DayPartitionScaleAccessor;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor.HourPartitionScaleAccessor;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor.HourTPartitionScaleAccessor;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor.MonthPartitionScaleAccessor;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor.ShortMonthPartitionScaleAccessor;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor.ShortYearPartitionScaleAccessor;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor.WeekPartitionScaleAccessor;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor.YearPartitionScaleAccessor;

public enum PartitionScale {
    HOUR(new HourPartitionScaleAccessor()),
    HOUR_T(new HourTPartitionScaleAccessor()),
    DAY(new DayPartitionScaleAccessor()),
    WEEK(new WeekPartitionScaleAccessor()),
    MONTH(new MonthPartitionScaleAccessor()),
    SHORT_MONTH(new ShortMonthPartitionScaleAccessor()),
    YEAR(new YearPartitionScaleAccessor()),
    SHORT_YEAR(new ShortYearPartitionScaleAccessor());

    private final PartitionScaleAccessor accessor;

    PartitionScale(PartitionScaleAccessor accessor) {
        this.accessor = accessor;
    }

    public PartitionScaleAccessor getAccessor() {
        return accessor;
    }
}
