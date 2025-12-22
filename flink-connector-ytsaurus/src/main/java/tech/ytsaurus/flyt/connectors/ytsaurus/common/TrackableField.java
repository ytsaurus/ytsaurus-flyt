package tech.ytsaurus.flyt.connectors.ytsaurus.common;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import tech.ytsaurus.flyt.connectors.ytsaurus.producer.converters.TrackableFieldDataConverter;

@Data
@AllArgsConstructor
public class TrackableField implements Serializable {

    private String name;

    private int index;

    private LogicalTypeRoot type;

    private TrackableFieldDataConverter converter;
}
