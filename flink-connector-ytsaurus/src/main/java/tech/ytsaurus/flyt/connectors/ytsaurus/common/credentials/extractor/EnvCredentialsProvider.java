package tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.extractor;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.OAuthCredentialsConfig;

public class EnvCredentialsProvider implements CredentialsProvider {
    private static final long serialVersionUID = 1L;

    public static final String IDENTIFIER = "env";

    private static final String YT_USERNAME_ENV = "YT_USERNAME";
    private static final String YT_TOKEN_ENV = "YT_TOKEN";

    @Override
    public String getProviderIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OAuthCredentialsConfig getCredentials(String clusterName) {
        return new OAuthCredentialsConfig(
                System.getenv(YT_USERNAME_ENV),
                System.getenv(YT_TOKEN_ENV));
    }
}
