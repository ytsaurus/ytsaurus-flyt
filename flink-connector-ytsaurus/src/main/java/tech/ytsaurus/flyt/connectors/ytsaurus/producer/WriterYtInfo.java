package tech.ytsaurus.flyt.connectors.ytsaurus.producer;

import lombok.Value;
import tech.ytsaurus.client.YTsaurusClient;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;

@Value
public class WriterYtInfo {
    ComplexYtPath path;
    YTsaurusClient client;
    String ysonSchemaString;
}
