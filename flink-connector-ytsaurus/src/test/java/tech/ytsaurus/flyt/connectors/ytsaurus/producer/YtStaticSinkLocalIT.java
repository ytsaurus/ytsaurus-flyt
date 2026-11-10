package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import tech.ytsaurus.client.TableReader;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.RemoveNode;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.extractor.ManualCredentialsProvider;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs only against an explicitly supplied disposable local YT instance; never a production cluster. */
@EnabledIfSystemProperty(named = "yt.static.test.proxy", matches = ".+")
class YtStaticSinkLocalIT {
    private static final int RECORDS = 2000;
    private static final AtomicBoolean INJECTED_FAILURE = new AtomicBoolean();

    @Test
    void fourParallelWritersAppendEveryRowToOneExactTable() throws Exception {
        run(false);
    }

    @Test
    void recoveryAfterCompletedCheckpointHasNoMissingRows() throws Exception {
        INJECTED_FAILURE.set(false);
        run(true);
        assertThat(INJECTED_FAILURE).isTrue();
    }

    private static void run(boolean failOnce) throws Exception {
        String proxy = System.getProperty("yt.static.test.proxy");
        if (!proxy.matches("(localhost|127\\.0\\.0\\.1):[0-9]+")) {
            throw new IllegalArgumentException("Local static-sink tests only allow localhost/127.0.0.1:port");
        }
        String path = "//tmp/flink_static_sink_" + UUID.randomUUID().toString().replace("-", "");
        try (YTsaurusClient client = YTsaurusClient.builder().setCluster(proxy)
                .setAuth(YTsaurusClientAuth.builder().setUser("root").setToken("local-test").build()).build()) {
            try {
                StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(4);
                env.enableCheckpointing(100);
                env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 10));
                env.fromSequence(0, RECORDS - 1).map(new FailAfterCheckpoint(failOnce))
                        .sinkTo(YtStaticSink.<Long>builder().setProxy(proxy).setPath(path)
                                .setYsonSchema(YtStaticSinkWriterTest.SCHEMA)
                                .setCredentialsProvider(new ManualCredentialsProvider("root", "local-test"))
                                .setRollInterval(Duration.ofMillis(200))
                                .setSerializer(value -> YTree.mapBuilder().key("id").value(value).buildMap())
                                .build())
                        .setParallelism(4);
                env.execute("static YT append/recovery test");

                List<Long> actual = new ArrayList<>();
                TableReader<YTreeMapNode> reader = client.readTable(ReadTable.builder(YTreeMapNode.class)
                        .setPath(YPath.simple(path)).build()).get(30, TimeUnit.SECONDS);
                try {
                    while (reader.canRead()) {
                        reader.readyEvent().get(30, TimeUnit.SECONDS);
                        List<YTreeMapNode> batch = reader.read();
                        if (batch != null) {
                            for (YTreeMapNode row : batch) {
                                actual.add(row.getLong("id"));
                            }
                        }
                    }
                } finally {
                    reader.close().get(30, TimeUnit.SECONDS);
                }
                assertThat(actual.stream().collect(Collectors.toSet()))
                        .containsExactlyInAnyOrderElementsOf(LongStream.range(0, RECORDS).boxed().collect(Collectors.toSet()));
                if (!failOnce) {
                    assertThat(actual).hasSize(RECORDS);
                }
                assertThat(client.getNode(path + "/@dynamic").get(30, TimeUnit.SECONDS).boolValue()).isFalse();
            } finally {
                client.removeNode(RemoveNode.builder().setPath(YPath.simple(path)).setForce(true).build())
                        .get(30, TimeUnit.SECONDS);
            }
        }
    }

    private static final class FailAfterCheckpoint extends RichMapFunction<Long, Long> implements CheckpointListener {
        private final boolean enabled;
        private transient boolean checkpointCompleted;

        private FailAfterCheckpoint(boolean enabled) { this.enabled = enabled; }

        @Override
        public Long map(Long value) throws Exception {
            if (enabled && checkpointCompleted && getRuntimeContext().getAttemptNumber() == 0
                    && INJECTED_FAILURE.compareAndSet(false, true)) {
                throw new IOExceptionForTest();
            }
            // Keep the bounded source alive long enough to exercise actual checkpoints.
            Thread.sleep(2);
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) { checkpointCompleted = true; }
    }

    private static final class IOExceptionForTest extends java.io.IOException {
        private IOExceptionForTest() { super("Injected failure after a completed checkpoint"); }
    }
}
