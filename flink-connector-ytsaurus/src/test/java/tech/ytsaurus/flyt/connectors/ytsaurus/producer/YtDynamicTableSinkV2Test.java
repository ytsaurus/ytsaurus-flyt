package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.junit.jupiter.api.Test;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.RowDataToYtListConverters;

import static org.assertj.core.api.Assertions.assertThat;

class YtDynamicTableSinkV2Test {
    @Test
    void exposesFlink2SinkV2Provider() {
        YtDynamicTableSink sink = YtDynamicTableSink.builder()
                .type(DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())))
                .ytConverters(new RowDataToYtListConverters(TimestampFormat.ISO_8601))
                .path(ComplexYtPath.builder()
                        .clusterName("local")
                        .basePath("//tmp/flink2-sink-test")
                        .build())
                .ysonSchemaString("[]")
                .build();

        Object provider = sink.getSinkRuntimeProvider(null);

        assertThat(provider).isInstanceOf(SinkV2Provider.class);
        assertThat(((SinkV2Provider) provider).createSink()).isInstanceOf(YtSink.class);
    }
}
