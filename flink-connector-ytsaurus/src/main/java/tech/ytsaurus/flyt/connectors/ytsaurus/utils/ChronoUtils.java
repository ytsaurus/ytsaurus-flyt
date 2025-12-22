package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import lombok.experimental.UtilityClass;
import org.apache.flink.table.data.TimestampData;

import static org.apache.flink.shaded.guava31.com.google.common.math.LongMath.checkedAdd;
import static org.apache.flink.shaded.guava31.com.google.common.math.LongMath.checkedMultiply;

@UtilityClass
public class ChronoUtils {
    public static final int MICROS_PER_MILLIS = 1000;
    public static final int NANOS_PER_MILLIS = 1000;

    /**
     * Get microseconds from the UNIX epoch start
     *
     * @param timestampData to get microseconds for
     * @return number of microseconds since epoch start
     * @implNote might overflow in the future
     */
    public static long getMicrosSinceEpoch(TimestampData timestampData) {
        return checkedAdd(
                checkedMultiply(timestampData.getMillisecond(), MICROS_PER_MILLIS),
                timestampData.getNanoOfMillisecond() / NANOS_PER_MILLIS);
    }

    /**
     * Get milliseconds from ISO date time
     */
    public static long getTimestampFromUtcDateTime(String etlUpdated) {
        return LocalDateTime.parse(etlUpdated, DateTimeFormatter.ISO_DATE_TIME)
                .atZone(ZoneId.of("UTC"))
                .toInstant()
                .toEpochMilli();
    }
}
