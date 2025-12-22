package tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.extractor;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials.OAuthCredentialsConfig;
import tech.ytsaurus.flyt.connectors.ytsaurus.utils.YtConfigUtils;

import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.CREDENTIALS_SOURCE;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.YT_TOKEN_OPTION;
import static tech.ytsaurus.flyt.connectors.ytsaurus.common.YtConnectorOptions.YT_USERNAME_OPTION;

public class OptionsCredentialsSourceTest {
    @Test
    public void extractCredentialsSuccess() {
        Configuration options = new Configuration();
        options.setString(YT_USERNAME_OPTION.key(), "sample_username");
        options.setString(YT_TOKEN_OPTION.key(), "sample_token");
        options.setString(CREDENTIALS_SOURCE.key(), OptionsCredentialsProvider.IDENTIFIER);
        OAuthCredentialsConfig credentials = YtConfigUtils.getAndValidateCredentialsProvider(options)
                .getCredentials("some_cluster");
        Assertions.assertEquals("sample_username", credentials.getUsername());
        Assertions.assertEquals("sample_token", credentials.getToken());
    }
}
