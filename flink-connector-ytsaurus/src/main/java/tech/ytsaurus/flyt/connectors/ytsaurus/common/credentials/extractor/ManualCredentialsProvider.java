package tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.extractor;

import lombok.RequiredArgsConstructor;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.CredentialsProvider;
import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.OAuthCredentialsConfig;

@RequiredArgsConstructor
public class ManualCredentialsProvider implements CredentialsProvider {
    private static final long serialVersionUID = 1L;

    private static final String IDENTIFIER = "manual";

    private final String username;
    private final String token;

    @Override
    public String getProviderIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OAuthCredentialsConfig getCredentials(String clusterName) {
        return new OAuthCredentialsConfig(username, token);
    }
}
