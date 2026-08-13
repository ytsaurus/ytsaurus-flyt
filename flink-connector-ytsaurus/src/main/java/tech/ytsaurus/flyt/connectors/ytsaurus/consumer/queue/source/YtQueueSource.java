package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import tech.ytsaurus.client.YTsaurusClient;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.YtQueueRecordDeserializer;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueStartupMode;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config.YtQueueTrimmedOffsetPolicy;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.YtQueueEnumeratorState;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.YtQueueEnumeratorStateSerializer;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.YtQueueSplitEnumerator;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.initializer.SpecificYtQueueOffsetInitializer;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.initializer.YTsaurusQueueOffsetInitializer;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.initializer.YtQueueOffsetInitializer;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.metadata.YTsaurusQueueMetadataProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.enumerator.metadata.YtQueueMetadataProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader.DirectYtQueuePullerFactory;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.reader.YtQueueSourceReader;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplit;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.source.split.YtQueueSplitSerializer;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtUtils;

import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.ASYNC_BUFFER_CAPACITY;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.ASYNC_WORKER_COUNT;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.MAX_DATA_WEIGHT;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.MAX_ROW_COUNT;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.PARTITION_DISCOVERY_INTERVAL;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.POLL_BACKOFF;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.STARTUP_MODE;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtQueueConnectorOptions.TRIMMED_OFFSET_POLICY;

public final class YtQueueSource<T>
        implements Source<T, YtQueueSplit, YtQueueEnumeratorState>,
        ResultTypeQueryable<T>, Serializable {
    private static final long serialVersionUID = 1L;

    private final String proxy;

    private final String queuePath;

    private final CredentialsProvider credentialsProvider;

    private final YtQueueRecordDeserializer<T> recordDeserializer;

    private final TypeInformation<T> producedType;

    private final YtQueueStartupMode startupMode;

    @Nullable
    private final List<Long> specificOffsets;

    private final YtQueueTrimmedOffsetPolicy trimmedOffsetPolicy;

    private final YtQueueReaderOptions readerOptions;

    private final long discoveryIntervalMillis;

    @SuppressWarnings("checkstyle:ParameterNumber")
    private YtQueueSource(
            String proxy,
            String queuePath,
            CredentialsProvider credentialsProvider,
            YtQueueRecordDeserializer<T> recordDeserializer,
            TypeInformation<T> producedType,
            YtQueueStartupMode startupMode,
            @Nullable List<Long> specificOffsets,
            YtQueueTrimmedOffsetPolicy trimmedOffsetPolicy,
            YtQueueReaderOptions readerOptions,
            Duration discoveryInterval) {
        this.proxy = requireNonBlank(proxy, "proxy");
        this.queuePath = requireNonBlank(queuePath, "queuePath");
        this.credentialsProvider = Objects.requireNonNull(credentialsProvider, "credentialsProvider");
        this.recordDeserializer = Objects.requireNonNull(recordDeserializer, "recordDeserializer");
        this.producedType = Objects.requireNonNull(producedType, "producedType");
        this.startupMode = Objects.requireNonNull(startupMode, "startupMode");
        this.specificOffsets = specificOffsets;
        this.trimmedOffsetPolicy = Objects.requireNonNull(trimmedOffsetPolicy, "trimmedOffsetPolicy");
        this.readerOptions = Objects.requireNonNull(readerOptions, "readerOptions");
        this.discoveryIntervalMillis = requirePositiveMilliseconds(discoveryInterval, "discoveryInterval");
        validateSpecificOffsets(startupMode, specificOffsets);
        if (trimmedOffsetPolicy != YtQueueTrimmedOffsetPolicy.FAIL) {
            throw new IllegalArgumentException("Unsupported trimmed offset policy: " + trimmedOffsetPolicy);
        }
    }

    private static void validateSpecificOffsets(
            YtQueueStartupMode startupMode,
            @Nullable List<Long> specificOffsets) {
        if (startupMode == YtQueueStartupMode.SPECIFIC) {
            if (specificOffsets == null || specificOffsets.isEmpty()) {
                throw new IllegalArgumentException(
                        "specificOffsets must contain at least one offset for SPECIFIC startup mode");
            }
            for (int index = 0; index < specificOffsets.size(); index++) {
                Long offset = specificOffsets.get(index);
                if (offset == null || offset < 0) {
                    throw new IllegalArgumentException(
                            "specificOffsets[" + index + "] must be nonnegative");
                }
            }
        } else if (specificOffsets != null) {
            throw new IllegalArgumentException("specificOffsets is only valid for SPECIFIC startup mode");
        }
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, YtQueueSplit> createReader(SourceReaderContext readerContext) throws Exception {
        DirectYtQueuePullerFactory pullerFactory = new DirectYtQueuePullerFactory(
                createClient(),
                queuePath);
        try {
            return new YtQueueSourceReader<>(
                    pullerFactory,
                    recordDeserializer,
                    readerOptions,
                    readerContext.getConfiguration(),
                    readerContext);
        } catch (Exception | Error failure) {
            closeAfterFailure(pullerFactory, failure);
            throw failure;
        }
    }

    @Override
    public SplitEnumerator<YtQueueSplit, YtQueueEnumeratorState> createEnumerator(
            SplitEnumeratorContext<YtQueueSplit> enumContext) {
        return createEnumerator(enumContext, YtQueueEnumeratorState.empty());
    }

    @Override
    public SplitEnumerator<YtQueueSplit, YtQueueEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<YtQueueSplit> enumContext,
            YtQueueEnumeratorState checkpoint) {
        return createEnumerator(enumContext, Objects.requireNonNull(checkpoint, "checkpoint"));
    }

    @Override
    public SimpleVersionedSerializer<YtQueueSplit> getSplitSerializer() {
        return new YtQueueSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<YtQueueEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new YtQueueEnumeratorStateSerializer();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return producedType;
    }

    private YtQueueSplitEnumerator createEnumerator(
            SplitEnumeratorContext<YtQueueSplit> enumContext,
            YtQueueEnumeratorState state) {
        YTsaurusClient metadataClient = createClient();
        YtQueueMetadataProvider metadataProvider = null;
        YtQueueOffsetInitializer offsetInitializer = null;
        try {
            metadataProvider = new YTsaurusQueueMetadataProvider(metadataClient, queuePath);
            if (startupMode == YtQueueStartupMode.SPECIFIC) {
                offsetInitializer = new SpecificYtQueueOffsetInitializer(specificOffsets);
            } else {
                offsetInitializer = new YTsaurusQueueOffsetInitializer(
                        createClient(),
                        queuePath,
                        startupMode);
            }
            return new YtQueueSplitEnumerator(
                    enumContext,
                    metadataProvider,
                    offsetInitializer,
                    discoveryIntervalMillis,
                    state);
        } catch (RuntimeException | Error failure) {
            closeAfterFailure(offsetInitializer, failure);
            if (metadataProvider == null) {
                closeAfterFailure(metadataClient, failure);
            } else {
                closeAfterFailure(metadataProvider, failure);
            }
            throw failure;
        }
    }

    private YTsaurusClient createClient() {
        return YtUtils.makeYtClient(proxy, credentialsProvider.getCredentials(proxy));
    }

    private static String requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value;
    }

    private static long requirePositiveMilliseconds(Duration value, String fieldName) {
        Objects.requireNonNull(value, fieldName);
        long milliseconds;
        try {
            milliseconds = value.toMillis();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(fieldName + " is too large", e);
        }
        if (milliseconds <= 0) {
            throw new IllegalArgumentException(fieldName + " must be at least one millisecond");
        }
        return milliseconds;
    }

    private static void closeAfterFailure(AutoCloseable closeable, Throwable failure) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    public static final class Builder<T> {
        private String proxy;

        private String queuePath;

        private CredentialsProvider credentialsProvider;

        private YtQueueRecordDeserializer<T> recordDeserializer;

        private TypeInformation<T> producedType;

        private YtQueueStartupMode startupMode = STARTUP_MODE.defaultValue();

        @Nullable
        private List<Long> specificOffsets;

        private YtQueueTrimmedOffsetPolicy trimmedOffsetPolicy = TRIMMED_OFFSET_POLICY.defaultValue();

        private int maxRowCount = MAX_ROW_COUNT.defaultValue();

        private long maxDataWeightBytes = MAX_DATA_WEIGHT.defaultValue().getBytes();

        private Duration pollBackoff = POLL_BACKOFF.defaultValue();

        private int workerCount = ASYNC_WORKER_COUNT.defaultValue();

        private int bufferCapacity = ASYNC_BUFFER_CAPACITY.defaultValue();

        @Nullable
        private YtQueueReaderOptions readerOptions;

        private Duration discoveryInterval = PARTITION_DISCOVERY_INTERVAL.defaultValue();

        public Builder<T> proxy(String proxy) {
            this.proxy = proxy;
            return this;
        }

        public Builder<T> queuePath(String queuePath) {
            this.queuePath = queuePath;
            return this;
        }

        public Builder<T> credentialsProvider(CredentialsProvider credentialsProvider) {
            this.credentialsProvider = credentialsProvider;
            return this;
        }

        public Builder<T> recordDeserializer(YtQueueRecordDeserializer<T> recordDeserializer) {
            this.recordDeserializer = recordDeserializer;
            return this;
        }

        public Builder<T> producedType(TypeInformation<T> producedType) {
            this.producedType = producedType;
            return this;
        }

        public Builder<T> startupMode(YtQueueStartupMode startupMode) {
            this.startupMode = startupMode;
            return this;
        }

        public Builder<T> specificOffsets(@Nullable List<Long> specificOffsets) {
            this.specificOffsets = specificOffsets;
            return this;
        }

        public Builder<T> trimmedOffsetPolicy(YtQueueTrimmedOffsetPolicy trimmedOffsetPolicy) {
            this.trimmedOffsetPolicy = trimmedOffsetPolicy;
            return this;
        }

        public Builder<T> maxRowCount(int maxRowCount) {
            this.maxRowCount = maxRowCount;
            this.readerOptions = null;
            return this;
        }

        public Builder<T> maxDataWeightBytes(long maxDataWeightBytes) {
            this.maxDataWeightBytes = maxDataWeightBytes;
            this.readerOptions = null;
            return this;
        }

        public Builder<T> pollBackoff(Duration pollBackoff) {
            this.pollBackoff = pollBackoff;
            this.readerOptions = null;
            return this;
        }

        public Builder<T> workerCount(int workerCount) {
            this.workerCount = workerCount;
            this.readerOptions = null;
            return this;
        }

        public Builder<T> bufferCapacity(int bufferCapacity) {
            this.bufferCapacity = bufferCapacity;
            this.readerOptions = null;
            return this;
        }

        public Builder<T> readerOptions(YtQueueReaderOptions readerOptions) {
            this.readerOptions = Objects.requireNonNull(readerOptions, "readerOptions");
            this.maxRowCount = readerOptions.getMaxRows();
            this.maxDataWeightBytes = readerOptions.getMaxDataWeightBytes();
            this.pollBackoff = readerOptions.getEmptyPollBackoff();
            this.workerCount = readerOptions.getWorkerCount();
            this.bufferCapacity = readerOptions.getBufferCapacity();
            return this;
        }

        public Builder<T> discoveryInterval(Duration discoveryInterval) {
            this.discoveryInterval = discoveryInterval;
            return this;
        }

        public YtQueueSource<T> build() {
            YtQueueReaderOptions resolvedReaderOptions = readerOptions;
            if (resolvedReaderOptions == null) {
                resolvedReaderOptions = new YtQueueReaderOptions(
                        maxRowCount,
                        maxDataWeightBytes,
                        pollBackoff,
                        workerCount,
                        bufferCapacity);
            }
            return new YtQueueSource<>(
                    proxy,
                    queuePath,
                    credentialsProvider,
                    recordDeserializer,
                    producedType,
                    startupMode,
                    specificOffsets,
                    trimmedOffsetPolicy,
                    resolvedReaderOptions,
                    discoveryInterval);
        }
    }
}
