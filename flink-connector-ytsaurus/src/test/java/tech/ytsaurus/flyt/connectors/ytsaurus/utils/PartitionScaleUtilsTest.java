package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionScale;

public class PartitionScaleUtilsTest {
    @ParameterizedTest
    @EnumSource(PartitionScale.class)
    public void testUnitEnd(PartitionScale partitionScale) {
        // The purpose of the test is to check that actual dates correspond to the meaning of PartitionScale,
        // the test does not check formatting

        Instant raw = LocalDateTime.parse("2020-05-16T14:08:13.31231").toInstant(ZoneOffset.UTC);
        String end = PartitionScaleUtils.formatYt(PartitionScaleUtils.getEnd(raw, partitionScale));

        switch (partitionScale) {
            case HOUR:
            case HOUR_T:
                Assertions.assertEquals("2020-05-16 15:00:00+00:00", end);
                break;
            case DAY:
                Assertions.assertEquals("2020-05-17 00:00:00+00:00", end);
                break;
            case WEEK:
                Assertions.assertEquals("2020-05-18 00:00:00+00:00", end);
                break;
            case YEAR:
            case SHORT_YEAR:
                Assertions.assertEquals("2021-01-01 00:00:00+00:00", end);
                break;
            case MONTH:
            case SHORT_MONTH:
                Assertions.assertEquals("2020-06-01 00:00:00+00:00", end);
                break;
            default:
                Assertions.fail(String.format("Unaccounted partition scale '%s'", partitionScale));
        }
    }
}
