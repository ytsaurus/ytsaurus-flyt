package tech.ytsaurus.flyt.connectors.ytsaurus.consumer.queue.config;

public enum YtQueueReadMode {
    /**
     * The whole queue row is a record: it is passed to the format as a YSON map.
     */
    ROW,

    /**
     * The record is a payload stored in a single queue column, optionally compressed with the codec
     * named in another column.
     */
    COLUMN
}
