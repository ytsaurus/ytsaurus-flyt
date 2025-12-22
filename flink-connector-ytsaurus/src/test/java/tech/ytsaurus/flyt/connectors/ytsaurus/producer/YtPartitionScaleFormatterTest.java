package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionScale;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.PartitionScaleAccessor;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor.StartDatePartitionScaleAccessor;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.partition.accessor.StartDateTimePartitionScaleAccessor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class YtPartitionScaleFormatterTest {
    @Test
    public void partitionScaleDayGetNameThenCheckSuccess() {
        PartitionScaleAccessor accessor = PartitionScale.DAY.getAccessor();
        assertEquals("2022-10-30", accessor.getName(
                LocalDate.of(2022, 10, 30)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
        assertEquals("2022-10-30", accessor.getName(
                ZonedDateTime.of(2022, 10, 30, 10, 10, 10, 10, ZoneOffset.UTC)
                        .toInstant()));
        assertEquals("2022-10-30", accessor.getName(
                LocalDateTime.of(2022, 10, 30, 10, 10, 10, 10)
                        .toInstant(ZoneOffset.UTC)));
    }

    @ParameterizedTest
    @CsvSource({
            "2022-10-30,2022-10-30T23:59:59.999999,2022-10-31",
            "2022-10-30,2022-10-30T00:00:00,2022-10-31",
            "1970-01-01,1970-01-01T00:00:00,1970-01-02"
    })
    public void partitionScaleDayCheckIntervalsSuccess(String start, String raw, String end) {
        testStartDatePartitionAccessor(
                (StartDatePartitionScaleAccessor) PartitionScale.DAY.getAccessor(),
                start, raw, end);
    }

    @Test
    public void partitionScaleWeekGetNameThenCheckSuccess() {
        PartitionScaleAccessor accessor = PartitionScale.WEEK.getAccessor();
        assertEquals("2023-05-29", accessor.getName(
                LocalDate.of(2023, 6, 2) // Nearest monday to 2nd of June is 29th of May (2023)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
        assertEquals("2023-05-29", accessor.getName(
                LocalDate.of(2023, 6, 1)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
        assertEquals("2023-05-29", accessor.getName(
                LocalDate.of(2023, 5, 30)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
        assertNotEquals("2023-05-29", accessor.getName(
                LocalDate.of(2023, 6, 5)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
    }

    @ParameterizedTest
    @CsvSource({
            "2024-04-15,2024-04-17T23:59:59.999999,2024-04-22",
            "2024-04-01,2024-04-07T23:59:59.999999,2024-04-08",
    })
    public void partitionScaleWeekCheckIntervalsSuccess(String start, String raw, String end) {
        testStartDatePartitionAccessor(
                (StartDatePartitionScaleAccessor) PartitionScale.WEEK.getAccessor(),
                start, raw, end);
    }

    @Test
    public void partitionScaleMonthGetNameThenCheckSuccess() {
        PartitionScaleAccessor accessor = PartitionScale.MONTH.getAccessor();
        assertEquals("2023-06-01", accessor.getName(
                LocalDate.of(2023, 6, 2)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
        assertEquals("2023-06-01", accessor.getName(
                LocalDate.of(2023, 6, 1)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
        assertEquals("2023-05-01", accessor.getName(
                LocalDate.of(2023, 5, 30)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
    }

    @Test
    public void partitionScaleShortMonthGetNameThenCheckSuccess() {
        PartitionScaleAccessor accessor = PartitionScale.SHORT_MONTH.getAccessor();
        assertEquals("2023-06", accessor.getName(
                LocalDate.of(2023, 6, 2)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
        assertEquals("2023-06", accessor.getName(
                LocalDate.of(2023, 6, 1)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
        assertEquals("2023-05", accessor.getName(
                LocalDate.of(2023, 5, 30)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
    }

    @ParameterizedTest
    @CsvSource({
            "2024-04-01,2024-04-17T23:59:59.999999,2024-05-01",
            "2024-04-01,2024-04-30T23:59:59.999999,2024-05-01",
            "2024-04-01,2024-04-01T00:00:00,2024-05-01",
    })
    public void partitionScaleMonthCheckIntervalsSuccess(String start, String raw, String end) {
        testStartDatePartitionAccessor(
                (StartDatePartitionScaleAccessor) PartitionScale.MONTH.getAccessor(),
                start, raw, end);
        testStartDatePartitionAccessor(
                (StartDatePartitionScaleAccessor) PartitionScale.SHORT_MONTH.getAccessor(),
                start, raw, end);
    }

    @Test
    public void partitionScaleYearGetNameThenCheckSuccess() {
        PartitionScaleAccessor accessor = PartitionScale.YEAR.getAccessor();
        assertEquals("2023-01-01", accessor.getName(
                LocalDate.of(2023, 6, 2)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
        assertEquals("2023-01-01", accessor.getName(
                LocalDate.of(2023, 4, 2)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
        assertEquals("2019-01-01", accessor.getName(
                LocalDate.of(2019, 1, 1)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
        assertEquals("2034-01-01", accessor.getName(
                LocalDate.of(2034, 1, 3)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
    }

    @Test
    public void partitionScaleShortYearGetNameThenCheckSuccess() {
        PartitionScaleAccessor accessor = PartitionScale.SHORT_YEAR.getAccessor();
        assertEquals("2023", accessor.getName(
                LocalDate.of(2023, 6, 2)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
        assertEquals("1970", accessor.getName(
                LocalDate.of(1970, 12, 31)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
        assertEquals("2019", accessor.getName(
                LocalDate.of(2019, 1, 1)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
        assertEquals("2034", accessor.getName(
                LocalDate.of(2034, 1, 3)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()));
    }

    @ParameterizedTest
    @CsvSource({
            "2024-01-01,2024-04-17T23:59:59.999999,2025-01-01",
            "2024-01-01,2024-12-31T23:59:59.999999,2025-01-01",
    })
    public void partitionScaleYearCheckIntervalsSuccess(String start, String raw, String end) {
        testStartDatePartitionAccessor(
                (StartDatePartitionScaleAccessor) PartitionScale.YEAR.getAccessor(),
                start, raw, end);
    }

    @Test
    public void partitionScaleHourGetNameThenCheckSuccess() {
        PartitionScaleAccessor accessor = PartitionScale.HOUR.getAccessor();
        Instant instant = LocalDateTime
                .of(2023, 1, 2, 3, 4, 5, 6)
                .atOffset(ZoneOffset.UTC)
                .toInstant();
        Assertions.assertEquals("2023-01-02 03:00:00", accessor.getName(instant));
    }


    @ParameterizedTest
    @CsvSource({
            "2024-01-01T10:00:00,2024-01-01T10:00:01,2024-01-01T11:00:00",
            "2024-01-01T10:00:00,2024-01-01T10:59:59.999999,2024-01-01T11:00:00",
    })
    public void partitionScaleHourCheckIntervalsSuccess(String start, String raw, String end) {
        testStartDateTimePartitionAccessor(
                (StartDateTimePartitionScaleAccessor) PartitionScale.HOUR.getAccessor(),
                start, raw, end);
        testStartDateTimePartitionAccessor(
                (StartDateTimePartitionScaleAccessor) PartitionScale.HOUR_T.getAccessor(),
                start, raw, end);
    }

    @Test
    public void partitionScaleHourTGetNameThenCheckSuccess() {
        PartitionScaleAccessor accessor = PartitionScale.HOUR_T.getAccessor();
        Instant instant = LocalDateTime
                .of(2023, 1, 2, 3, 4, 5, 6)
                .atOffset(ZoneOffset.UTC)
                .toInstant();
        Assertions.assertEquals("2023-01-02T03:00:00", accessor.getName(instant));
    }

    private void testStartDatePartitionAccessor(StartDatePartitionScaleAccessor accessor,
                                                String start,
                                                String raw,
                                                String end) {
        LocalDate startDate = LocalDate.parse(start);
        LocalDate endDate = LocalDate.parse(end);

        Instant current = LocalDateTime.parse(raw).toInstant(ZoneOffset.UTC);

        LocalDate actualStart = accessor.getStart(current);
        LocalDate actualEnd = accessor.getEnd(current);

        Assertions.assertEquals(startDate, actualStart);
        Assertions.assertEquals(endDate, actualEnd);
        Assertions.assertFalse(startDate.isAfter(LocalDate.ofInstant(current, ZoneOffset.UTC)));
        Assertions.assertTrue(endDate.isAfter(LocalDate.ofInstant(current, ZoneOffset.UTC)));
        Assertions.assertTrue(endDate.isAfter(startDate));
    }

    private void testStartDateTimePartitionAccessor(StartDateTimePartitionScaleAccessor accessor,
                                                    String start,
                                                    String raw,
                                                    String end) {
        LocalDateTime startDateTime = LocalDateTime.parse(start);
        LocalDateTime endDateTime = LocalDateTime.parse(end);

        Instant current = LocalDateTime.parse(raw).toInstant(ZoneOffset.UTC);

        LocalDateTime actualStart = accessor.getStart(current);
        LocalDateTime actualEnd = accessor.getEnd(current);

        Assertions.assertEquals(startDateTime, actualStart);
        Assertions.assertEquals(endDateTime, actualEnd);
        Assertions.assertFalse(startDateTime.isAfter(LocalDateTime.ofInstant(current, ZoneOffset.UTC)));
        Assertions.assertTrue(endDateTime.isAfter(LocalDateTime.ofInstant(current, ZoneOffset.UTC)));
        Assertions.assertTrue(endDateTime.isAfter(startDateTime));
    }
}
