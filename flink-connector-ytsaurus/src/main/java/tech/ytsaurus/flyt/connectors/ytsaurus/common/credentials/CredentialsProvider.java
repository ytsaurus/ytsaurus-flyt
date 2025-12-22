package tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials;

import java.io.Serializable;

import org.apache.flink.configuration.ReadableConfig;

public interface CredentialsProvider extends Serializable {

    /**
     * @return a unique identifier among all credentials provider implementations.
     */
    String getProviderIdentifier();

    /**
     * Initialize the provider with connector's options
     *
     * @param config to initialize with
     * @apiNote it must be guaranteed that this is called before {@link #getCredentials(String)}
     */
    default void init(ReadableConfig config) {
    }

    /**
     * Extract credentials from some source (environment, file, etc.)
     *
     * @return config with filled credentials
     */
    OAuthCredentialsConfig getCredentials(String clusterName);
}
