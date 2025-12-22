package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.InstantRowDataConverter;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.YtPartitioningInstantRowDataConverter;

public class YtInstantConverterTest {
    @Test
    void convertFromDateThenCheckSuccess() {
        InstantRowDataConverter converter = new YtPartitioningInstantRowDataConverter(new DateType());
        LocalDate source = LocalDate.of(2022, 10, 10);
        GenericRowData genericRowData = new GenericRowData(1);
        genericRowData.setField(0, (int) source.toEpochDay());
        Instant converted = converter.convert(genericRowData, 0);
        Assertions.assertEquals(source.atStartOfDay(ZoneOffset.UTC).toInstant(), converted);
    }

    @Test
    void convertFromTimeThenCheckSuccess() {
        InstantRowDataConverter converter = new YtPartitioningInstantRowDataConverter(new TimeType());
        LocalTime source = LocalTime.of(10, 10, 10);
        GenericRowData genericRowData = new GenericRowData(1);
        genericRowData.setField(0, (int) (source.toNanoOfDay() / 1_000_000));
        Instant converted = converter.convert(genericRowData, 0);
        Assertions.assertEquals(source.atDate(LocalDate.MIN).toInstant(ZoneOffset.UTC), converted);
    }

    @Test
    void convertFromTimestampThenCheckSuccess() {
        InstantRowDataConverter converter = new YtPartitioningInstantRowDataConverter(new TimestampType());
        ZonedDateTime source = ZonedDateTime.of(2022, 10, 10, 10, 10, 10, 10, ZoneOffset.UTC);
        GenericRowData genericRowData = new GenericRowData(1);
        genericRowData.setField(0, TimestampData.fromInstant(source.toInstant()));
        Instant converted = converter.convert(genericRowData, 0);
        Assertions.assertEquals(source.toInstant(), converted);
    }
}
