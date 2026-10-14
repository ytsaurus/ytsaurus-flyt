package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import tech.ytsaurus.flyt.connectors.datametrics.DataMetricsConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.TrackableField;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;

/**
 * At-least-once append to one unsorted static YT table.
 *
 * <p>A successful checkpoint waits for every preceding upload to commit. Completed uploads
 * are visible before the checkpoint completes, and recovery can append duplicates.
 */
public final class YtStaticSink<T> implements Sink<T>, SupportsPreWriteTopology<T> {
    private static final long serialVersionUID = 1L;
    public static final Duration DEFAULT_ROLL_INTERVAL = Duration.ofSeconds(60);
    public static final Duration DEFAULT_WRITE_TIMEOUT = Duration.ofMinutes(5);

    final String proxy;
    final String path;
    final String ysonSchema;
    final CredentialsProvider credentialsProvider;
    final YtStaticRowSerializer<T> serializer;
    final Duration rollInterval;
    final Duration writeTimeout;
    final String proxyRole;
    final boolean useTls;
    final Map<String, String> customAttributes;
    final RowType rowType;
    final DataMetricsConfig dataMetricsConfig;
    final TrackableField trackableField;

    private YtStaticSink(Builder<T> builder) {
        proxy = requireNonBlank(builder.proxy, "proxy");
        path = requireNonBlank(builder.path, "path");
        if (!path.startsWith("//") || path.endsWith("/") || path.contains("/@") || path.contains("[")) {
            throw new IllegalArgumentException("path must be an exact absolute YT table path");
        }
        ysonSchema = Objects.requireNonNull(builder.ysonSchema, "schema");
        YtStaticTableClient.parseSchema(ysonSchema);
        credentialsProvider = Objects.requireNonNull(builder.credentialsProvider, "credentialsProvider");
        serializer = Objects.requireNonNull(builder.serializer, "serializer");
        rollInterval = Objects.requireNonNull(builder.rollInterval, "rollInterval");
        writeTimeout = Objects.requireNonNull(builder.writeTimeout, "writeTimeout");
        if (rollInterval.toMillis() < 1 || writeTimeout.toMillis() < 1
                || rollInterval.compareTo(writeTimeout) >= 0) {
            throw new IllegalArgumentException("Require 0 < sink.roll-interval < sink.write-timeout (at least 1 ms)");
        }
        // Check representability before calculating monotonic deadlines at runtime.
        writeTimeout.toNanos();
        proxyRole = builder.proxyRole;
        useTls = builder.useTls;
        customAttributes = Collections.unmodifiableMap(new HashMap<>(builder.customAttributes));
        YtStaticTableClient.validateCustomAttributes(customAttributes);
        rowType = builder.rowType;
        dataMetricsConfig = builder.dataMetricsConfig;
        trackableField = builder.trackableField;
        if (rowType == null && (dataMetricsConfig != null || trackableField != null)) {
            throw new IllegalArgumentException("RowData metrics require the forRowData builder");
        }
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    public static Builder<RowData> forRowData(RowType rowType, String ysonSchema) {
        Builder<RowData> builder = new Builder<>();
        builder.rowType = Objects.requireNonNull(rowType);
        return builder.setYsonSchema(ysonSchema).setSerializer(
                YtStaticRowSerializer.forRowData(rowType, ysonSchema, TimestampFormat.ISO_8601));
    }

    @Override
    public DataStream<T> addPreWriteTopology(DataStream<T> input) {
        if (!input.getExecutionEnvironment().getCheckpointConfig().isCheckpointingEnabled()) {
            throw new IllegalStateException("Static YT sink requires Flink checkpointing for at-least-once delivery");
        }
        return input;
    }

    @Override
    public SinkWriter<T> createWriter(InitContext context) throws IOException {
        return new YtStaticSinkWriter<>(this, context, () -> new YtStaticTableClient(this));
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    public static final class Builder<T> {
        private String proxy;
        private String path;
        private String ysonSchema;
        private CredentialsProvider credentialsProvider;
        private YtStaticRowSerializer<T> serializer;
        private Duration rollInterval = DEFAULT_ROLL_INTERVAL;
        private Duration writeTimeout = DEFAULT_WRITE_TIMEOUT;
        private String proxyRole;
        private boolean useTls;
        private Map<String, String> customAttributes = Collections.emptyMap();
        private RowType rowType;
        private DataMetricsConfig dataMetricsConfig;
        private TrackableField trackableField;

        public Builder<T> setProxy(String value) { proxy = value; return this; }
        public Builder<T> setPath(String value) { path = value; return this; }
        public Builder<T> setYsonSchema(String value) { ysonSchema = value; return this; }
        public Builder<T> setCredentialsProvider(CredentialsProvider value) {
            credentialsProvider = value;
            return this;
        }
        public Builder<T> setSerializer(YtStaticRowSerializer<T> value) { serializer = value; return this; }
        public Builder<T> setRollInterval(Duration value) { rollInterval = value; return this; }
        public Builder<T> setWriteTimeout(Duration value) { writeTimeout = value; return this; }
        public Builder<T> setProxyRole(String value) { proxyRole = value; return this; }
        public Builder<T> setUseTls(boolean value) { useTls = value; return this; }
        public Builder<T> setCustomAttributes(Map<String, String> value) {
            customAttributes = new HashMap<>(value);
            return this;
        }
        public Builder<T> setDataMetricsConfig(DataMetricsConfig value) { dataMetricsConfig = value; return this; }
        public Builder<T> setTrackableField(TrackableField value) { trackableField = value; return this; }

        @SuppressWarnings("unchecked")
        public Builder<T> setTimestampFormat(TimestampFormat value) {
            if (rowType == null) {
                throw new IllegalArgumentException("Timestamp format requires the forRowData builder");
            }
            serializer = (YtStaticRowSerializer<T>) YtStaticRowSerializer.forRowData(rowType, ysonSchema, value);
            return this;
        }

        public YtStaticSink<T> build() { return new YtStaticSink<>(this); }
    }
}
