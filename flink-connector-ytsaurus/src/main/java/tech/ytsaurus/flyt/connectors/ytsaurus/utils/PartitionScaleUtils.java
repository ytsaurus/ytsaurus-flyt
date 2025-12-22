package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.Temporal;

import lombok.experimental.UtilityClass;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionScale;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

@UtilityClass
public class PartitionScaleUtils {
    public static final ZoneOffset DEFAULT_ZONE_OFFSET = ZoneOffset.UTC;

    private static final DateTimeFormatter YT_DATE_TIME_FORMATTER =
            new DateTimeFormatterBuilder()
                    .parseCaseInsensitive()
                    .append(DateTimeFormatter.ISO_LOCAL_DATE)
                    .appendLiteral(' ')
                    .append(new DateTimeFormatterBuilder()
                            .parseCaseInsensitive()
                            .append(ISO_LOCAL_TIME)
                            .appendOffset("+HH:MM:ss", "+00:00")
                            .toFormatter())
                    .toFormatter();

    public static LocalDate getLocalDate(Instant instant) {
        return LocalDate.ofInstant(instant, DEFAULT_ZONE_OFFSET);
    }

    public static LocalDateTime getLocalDateTime(Instant instant) {
        return LocalDateTime.ofInstant(instant, DEFAULT_ZONE_OFFSET);
    }

    public static String formatYt(OffsetDateTime offsetDateTime) {
        return YT_DATE_TIME_FORMATTER.format(offsetDateTime);
    }

    public static OffsetDateTime getEnd(Instant raw, PartitionScale partitionScale) {
        Temporal temporal = partitionScale.getAccessor().getEnd(raw);
        if (temporal instanceof LocalDate) {
            return ((LocalDate) temporal).atStartOfDay().atOffset(DEFAULT_ZONE_OFFSET);
        }
        if (temporal instanceof LocalDateTime) {
            return ((LocalDateTime) temporal).atOffset(DEFAULT_ZONE_OFFSET);
        }
        throw new IllegalArgumentException(String.format(
                "Partition scale accessor result must be LocalDate or LocalDateTime but got '%s'",
                temporal.getClass()));
    }
}
