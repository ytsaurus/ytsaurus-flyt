package tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.extractor;

import org.apache.flink.configuration.ReadableConfig;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.OAuthCredentialsConfig;

import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.YT_TOKEN_OPTION;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.YT_USERNAME_OPTION;

public class OptionsCredentialsProvider implements CredentialsProvider {
    private static final long serialVersionUID = 1L;

    public static final String IDENTIFIER = "options";

    private String username;
    private String token;

    @Override
    public String getProviderIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public void init(ReadableConfig config) {
        this.username = config.get(YT_USERNAME_OPTION);
        this.token = config.get(YT_TOKEN_OPTION);
    }

    @Override
    public OAuthCredentialsConfig getCredentials(String clusterName) {
        return new OAuthCredentialsConfig(username, token);
    }
}
