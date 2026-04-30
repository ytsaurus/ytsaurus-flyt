package tech.ytsaurus.flyt.connectors.ytsaurus.consumer;

import java.util.Map;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.consumer.cluster.ClusterPickStrategy;

@Slf4j
public class YtRowDataMultiClusterInputFormat extends AbstractYtRowDataInputFormat {
    private static final long serialVersionUID = 1L;

    private final Map<String, ComplexYtPath> pathMap;
    private final ClusterPickStrategy clusterPickStrategy;

    @Builder
    public YtRowDataMultiClusterInputFormat(
            Map<String, ComplexYtPath> pathMap,
            ClusterPickStrategy clusterPickStrategy,
            String ysonSchemaString,
            long limit,
            DeserializationSchema<RowData> deserializer,
            TypeInformation<RowData> rowDataTypeInfo,
            CredentialsProvider credentialsProvider) {

        super(ysonSchemaString, limit, deserializer, rowDataTypeInfo, credentialsProvider);
        this.pathMap = pathMap;
        this.clusterPickStrategy = clusterPickStrategy;
    }

    @Override
    protected ComplexYtPath resolvePath() {
        String cluster = clusterPickStrategy.pickCluster();
        ComplexYtPath path = pathMap.get(cluster);
        log.info("Strategy {} picked cluster {}, so path is: {}",
                clusterPickStrategy.getName(),
                cluster,
                path);
        return path;
    }


    @Override
    public void configure(Configuration parameters) {
        // do nothing here
    }
}
