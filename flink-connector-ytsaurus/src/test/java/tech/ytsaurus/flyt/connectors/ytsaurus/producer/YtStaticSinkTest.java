package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Duration;
import java.util.List;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import tech.ytsaurus.flyt.connectors.datametrics.DataMetricsConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class YtStaticSinkTest {
    @Test
    void refusesTopologyWithoutCheckpointsBeforeCreatingAnyClientOrTable() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Long> input = env.fromElements(1L);
        YtStaticSink<Long> sink = YtStaticSinkWriterTest.builder().build();
        assertThrows(IllegalStateException.class, () -> sink.addPreWriteTopology(input));
        env.enableCheckpointing(1000);
        assertThat(sink.addPreWriteTopology(input)).isSameAs(input);
    }

    @Test
    void rejectsRowDataMetricsOnGenericBuilderAndInvalidTimeouts() {
        assertThrows(IllegalArgumentException.class, () -> YtStaticSinkWriterTest.builder()
                .setDataMetricsConfig(new DataMetricsConfig("table", List.of())).build());
        assertThrows(IllegalArgumentException.class, () -> YtStaticSinkWriterTest.builder()
                .setRollInterval(Duration.ofMinutes(5)).build());
        assertThrows(IllegalArgumentException.class, () -> YtStaticSinkWriterTest.builder()
                .setRollInterval(Duration.ofNanos(1)).build());
    }
}
