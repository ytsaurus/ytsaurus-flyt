package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import lombok.experimental.UtilityClass;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.CLUSTER_NAME;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.CREDENTIALS_SOURCE;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.ENABLE_DYNAMIC_STORE_READ;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PARTITION_KEY;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PATH;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.PATH_MAP;

@UtilityClass
public class YtConfigUtils {
    public static Map<String, ComplexYtPath> getPathMap(ReadableConfig options) {
        Map<String, String> pathMap;
        if (options.getOptional(CLUSTER_NAME).isPresent()) {
            checkArgument(options.getOptional(PATH_MAP).isEmpty(),
                    "Path map must be null when cluster name is used");
            pathMap = Map.of(options.get(CLUSTER_NAME), options.get(PATH));
        } else {
            pathMap = options.get(PATH_MAP);
            checkNotNull(pathMap, "Path map or cluster name must be present");
        }
        return pathMap.entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey().toUpperCase(Locale.ROOT),
                        entry -> makeComplexPath(
                                entry.getValue(),
                                entry.getKey(),
                                options.getOptional(PARTITION_KEY).isPresent(),
                                options
                        )));
    }

    public static ComplexYtPath makeComplexPath(String path,
                                                String clusterName,
                                                boolean partitioned,
                                                ReadableConfig options) {
        return ComplexYtPath.builder()
                .basePath(path)
                .clusterName(clusterName)
                .isPartitioned(partitioned)
                .enableDynamicStoreRead(options.get(ENABLE_DYNAMIC_STORE_READ))
                .build();
    }

    public static CredentialsProvider getAndValidateCredentialsProvider(ReadableConfig options) {
        String requiredCredentialsSource = options.get(CREDENTIALS_SOURCE).toLowerCase();
        ServiceLoader<CredentialsProvider> credentialsProviderLoader = ServiceLoader.load(CredentialsProvider.class);
        for (CredentialsProvider credentialsProvider : credentialsProviderLoader) {
            if (credentialsProvider.getProviderIdentifier().equals(requiredCredentialsSource)) {
                credentialsProvider.init(options);
                return credentialsProvider;
            }
        }
        throw new ValidationException("Couldn't find CredentialsProvider with identifier: "
                + requiredCredentialsSource);
    }
}
