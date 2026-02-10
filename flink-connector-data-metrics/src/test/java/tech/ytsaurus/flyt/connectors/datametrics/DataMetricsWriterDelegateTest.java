package tech.ytsaurus.flyt.connectors.datametrics;

import java.util.List;
import java.util.Map;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class DataMetricsWriterDelegateTest {
    @Test
    void createReturnsNoopForNullOrEmptyConfig() {
        DataType dataType = DataTypes.ROW(DataTypes.FIELD("event_ts", DataTypes.BIGINT()));

        Assertions.assertInstanceOf(
                NoopDataMetricsWriterDelegate.class,
                DataMetricsWriterDelegate.create(null, dataType)
        );

        DataMetricsConfig emptyConfig = new DataMetricsConfig("alias", List.of());
        Assertions.assertInstanceOf(
                NoopDataMetricsWriterDelegate.class,
                DataMetricsWriterDelegate.create(emptyConfig, dataType)
        );
    }

    @Test
    void collectsMaxValuesAndClearsOnCommit() {
        DataType dataType = DataTypes.ROW(DataTypes.FIELD("event_ts", DataTypes.BIGINT()));
        DataMetricsMetricConfig metric = DataMetricsMetricConfig.builder()
                .metricName("event_ts")
                .columnName("event_ts")
                .build();
        DataMetricsConfig config = new DataMetricsConfig("alias", List.of(metric));

        DataMetricsWriterDelegate delegate = DataMetricsWriterDelegate.create(config, dataType);

        MetricGroup rootGroup = Mockito.mock(MetricGroup.class);
        MetricGroup dataMetricsGroup = Mockito.mock(MetricGroup.class);
        MetricGroup aliasGroup = Mockito.mock(MetricGroup.class);
        Mockito.when(rootGroup.addGroup("data_metrics")).thenReturn(dataMetricsGroup);
        Mockito.when(dataMetricsGroup.addGroup("alias")).thenReturn(aliasGroup);
        Mockito.when(aliasGroup.gauge(Mockito.anyString(), Mockito.any())).thenAnswer(invocation -> invocation.getArgument(1));

        delegate.open(rootGroup);

        GenericRowData first = new GenericRowData(1);
        first.setField(0, 10L);
        GenericRowData second = new GenericRowData(1);
        second.setField(0, 20L);

        delegate.onRecord(first);
        delegate.onRecord(second);

        Map<String, Long> pendingValues = delegate.getPendingValues();
        Assertions.assertEquals(20L, pendingValues.get("event_ts"));

        delegate.onCommit();

        pendingValues = delegate.getPendingValues();
        Assertions.assertTrue(pendingValues.isEmpty());
        Assertions.assertTrue(delegate.isInitialized());
    }
}
