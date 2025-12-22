package tech.ytsaurus.flyt.connectors.ytsaurus.common;

import java.io.Serializable;

public interface YtRowFilter<T> extends Serializable {
    boolean filter(T row);
}
