package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue;

import java.io.Serializable;

import org.apache.flink.api.connector.source.SourceReaderContext;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;

@FunctionalInterface
public interface YtQueueRecordDeserializer<T> extends Serializable {
    default void open(SourceReaderContext context) throws Exception {
    }

    T deserialize(UnversionedRow row, TableSchema schema) throws Exception;
}
