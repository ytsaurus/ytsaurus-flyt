package tech.ytsaurus.flyt.connectors.ytsaurus.common;

import java.io.Serializable;

import tech.ytsaurus.client.rows.UnversionedRow;

public interface YtRowDeserializer<T> extends Serializable {
    T deserialize(UnversionedRow row);
}
