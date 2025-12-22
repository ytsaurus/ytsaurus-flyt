package tech.ytsaurus.flyt.connectors.ytsaurus.utils;

import java.util.concurrent.ExecutionException;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import tech.ytsaurus.client.ProxySelector;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.YTsaurusClientConfig;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.ComplexYtPath;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.YtClientConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.OAuthCredentialsConfig;

import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_ATTRIBUTE_SYMBOL;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_PATH_SEP;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.constants.YtConsts.YT_SCHEMA_ATTRIBUTE_NAME;

@UtilityClass
public final class YtUtils {

    // Custom RPC ProxySelector class for Yt client.
    // Used in integration / e2e tests to override default PRC Proxy URL, because YT ports are defined in runtime.
    public static final String RPC_PROXY_SELECTOR_CLASS_PROPERTY = "yt.rpc-proxy-selector-class";

    /**
     * Create YT client config builder with basic parameters
     *
     * @param cluster  to get client config for
     * @param username username to access YT
     * @param token    token to access YT
     * @return client config builder
     */
    private static YtClientConfig.Builder createYtClientConfigBuilder(String cluster, String username, String token) {
        return new YtClientConfig.Builder()
                .setUser(username)
                .setToken(token)
                .setProxy(cluster);
    }

    /**
     * Get YT client config by username and token
     *
     * @param cluster  to get client config for
     * @param username username to access YT
     * @param token    token to access YT
     * @return client config
     */
    public static YtClientConfig getYtClientConfig(String cluster, String username, String token) {
        return createYtClientConfigBuilder(cluster, username, token)
                .build();
    }

    /**
     * Get YT client config by username, token and proxy role
     *
     * @param cluster   to get client config for
     * @param username  username to access YT
     * @param token     token to access YT
     * @param proxyRole proxy role for YT access
     * @return client config
     */
    public static YtClientConfig getYtClientConfig(String cluster, String username, String token, String proxyRole) {
        return createYtClientConfigBuilder(cluster, username, token)
                .setProxyRole(proxyRole)
                .build();
    }

    /**
     * Fetch YSON schema string from YT.
     *
     * @param pathToTable  path to table to fetch schema from
     * @param clientConfig yt client config
     * @return String YSON schema of the table
     */
    public static String fetchSchemaStringFromTable(String pathToTable, YtClientConfig clientConfig)
            throws ExecutionException, InterruptedException {
        try (YTsaurusClient client = makeYtClient(clientConfig.getProxy(), clientConfig.getUser(), clientConfig.getToken())) {
            return YTreeTextSerializer.serialize(
                    client.getNode(pathToTable + YT_PATH_SEP + YT_ATTRIBUTE_SYMBOL + YT_SCHEMA_ATTRIBUTE_NAME)
                            .get()
            );
        }
    }

    /**
     * Fetch YSON schema string from YT.
     * Table from which the schema is fetched must be placed inside the catalog.
     * Moreover, it must be named exactly as the catalog.
     * <p>
     * For example, if catalog is located at {@code //home/ytsaurus/tutorial}
     * then the table must be located at {@code //home/ytsaurus/tutorial/tutorial}
     *
     * @param pathToCatalog path to catalog with table to fetch schema from
     * @param clientConfig  yt client config
     * @return String YSON schema of the table
     */
    public static String fetchSchemaStringFromCatalog(String pathToCatalog, YtClientConfig clientConfig)
            throws ExecutionException, InterruptedException {
        String[] parts = pathToCatalog.split(YT_PATH_SEP);
        String tableName = pathToCatalog + YT_PATH_SEP + parts[parts.length - 1];
        return fetchSchemaStringFromTable(tableName, clientConfig);
    }

    /**
     * Creates YT client based on cluster address and credentials.
     */
    public static YTsaurusClient makeYtClient(ComplexYtPath path, OAuthCredentialsConfig credentialsConfig) {
        return makeYtClient(path.getClusterName(), credentialsConfig.getUsername(), credentialsConfig.getToken());
    }

    public static YTsaurusClient.ClientBuilder<? extends YTsaurusClient, ?> makeYtClientBuilder(
            ComplexYtPath path,
            OAuthCredentialsConfig credentialsConfig) {
        return makeYtClientBuilder(path.getClusterName(), credentialsConfig.getUsername(),
                credentialsConfig.getToken());
    }

    private static YTsaurusClient makeYtClient(String clusterName, String username, String token) {
        return makeYtClientBuilder(clusterName, username, token).build();
    }

    @SneakyThrows
    private static YTsaurusClient.ClientBuilder<? extends YTsaurusClient, ?> makeYtClientBuilder(String cluster,
                                                                                                 String username,
                                                                                                 String token) {
        YTsaurusClient.ClientBuilder<? extends YTsaurusClient, ?> builder = YTsaurusClient.builder()
                .setCluster(cluster)
                .setAuth(YTsaurusClientAuth.builder()
                        .setUser(username)
                        .setToken(token)
                        .build());
        String rpcProxySelectorClass = System.getProperty(RPC_PROXY_SELECTOR_CLASS_PROPERTY);
        if (rpcProxySelectorClass != null) {
            ProxySelector proxySelector = (ProxySelector)
                    Class.forName(rpcProxySelectorClass).getDeclaredConstructor().newInstance();
            builder.setConfig(YTsaurusClientConfig.builder()
                    .setRpcOptions(new RpcOptions().setRpcProxySelector(proxySelector))
                    .build());
        }
        return builder;
    }
}
